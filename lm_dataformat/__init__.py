import os
import zstandard
import json
import time
import tarfile
import codecs
from functools import reduce
import jsonlines
import io
from zipfile import ZipFile
import gzip
from math import ceil


def listdir_or_file(x):
    if isinstance(x, list):
        return reduce(lambda x,y:x+y, map(listdir_or_file, x))
    return [x] if os.path.isfile(x) else [x + '/' + fn for fn in os.listdir(x)]

def tarfile_reader(file):
    # we need our own tarfile parser because `tarfile` doesn't work well for 
    # big tarfiles; it seems to be reading the entire file to get a list of 
    # where all the files are - but we don't need that because we just need 
    # to see each file once. surprisingly, `tarfile` doesn't expose any 
    # facilities for this. the only options are 1. load the entire tarfile 
    # and then query by filename or 2. extract to disk - and neither of 
    # these is what we want.
    while True:
        hdr = file.read(512)

        # https://www.gnu.org/software/tar/manual/html_node/Standard.html
        # end at 135 not 136 because of \0 terminator
        if hdr[124:135] == b'\0'*11:
            # end of record
            break
        size = int(hdr[124:135], 8)

        padded_size = ceil(size / 512) * 512

        yield file.read(padded_size)[:size]

def handle_jsonl(jsonl_reader, get_meta, autojoin_paragraphs, para_joiner):
    for ob in jsonl_reader:
        # naive jsonl where each object is just the string itself, with no meta. For legacy compatibility.
        if isinstance(ob, str):
            assert not get_meta
            yield ob
            continue

        text = ob['text']

        if autojoin_paragraphs and isinstance(text, list):
            text = para_joiner.join(text)

        if get_meta:
            yield text, (ob['meta'] if 'meta' in ob else {})
        else:
            yield text


class Reader:
    def __init__(self, in_path):
        self.in_path = in_path
    
    def stream_data(self, get_meta=False):
        for f in listdir_or_file(self.in_path):
            if f == 'openwebtext.tar.xz':
                assert not get_meta

                yield from self.read_owt(f)
            elif 'urlsf_subset' in f and f.endswith('_data.xz'):
                assert not get_meta

                yield from self.read_owt_subset(f)
            elif f.endswith('.dat.zst'):
                assert not get_meta

                yield from self.read_dat(f)
            elif f.endswith('.jsonl.zst'):
                yield from self.read_jsonl(f, get_meta)
            elif f.endswith('.jsonl.zst.tar'):
                yield from self.read_jsonl_tar(f, get_meta)
            elif f.endswith('.json.zst'):
                assert not get_meta

                yield from self.read_json(f)
            elif f.endswith('.txt'):
                assert not get_meta
                
                yield from self.read_txt(f)
            elif f.endswith('.zip'):
                assert not get_meta

                yield from self.read_zip(f)
            elif f.endswith('.tar.gz'):
                assert not get_meta

                yield from self.read_tgz(f)

    def read_txt(self, file):
        with open(file, 'r') as fh:
            yield fh.read()

    def read_zip(self, file):
        archive = ZipFile(file, 'r')
        for f in archive.namelist():
            yield archive.read(f).decode('UTF-8')

    def read_tgz(self, file):
        gz = gzip.open(file)
        yield from (x.decode('utf-8') for x in tarfile_reader(gz))

    def read_json(self, file):
        with open(file, 'rb') as fh:
            cctx = zstandard.ZstdDecompressor()
            reader = cctx.stream_reader(fh)
            ob = json.load(reader)
            yield from ob

    def read_dat(self, file):
        with open(file, 'rb') as fh:
            cctx = zstandard.ZstdDecompressor()
            reader = cctx.stream_reader(fh)
            while True:
                ln = reader.read(16).decode('UTF-8')
                if not ln:
                    break

                ln = int(ln)

                yield reader.read(ln).decode('UTF-8')

    def read_jsonl(self, file, get_meta=False, autojoin_paragraphs=True, para_joiner='\n\n'):
        with open(file, 'rb') as fh:
            cctx = zstandard.ZstdDecompressor()
            reader = io.BufferedReader(cctx.stream_reader(fh))
            rdr = jsonlines.Reader(reader)
            yield from handle_jsonl(rdr, get_meta, autojoin_paragraphs, para_joiner)


    def read_jsonl_tar(self, file, get_meta=False, autojoin_paragraphs=True, para_joiner='\n\n'):
        with open(file, 'rb') as fh:
            for f in tarfile_reader(fh):
                # TODO: implement proper streaming so we don't have to read everything into memory at once
                fh = io.BytesIO(f)
                fh.seek(0)

                cctx = zstandard.ZstdDecompressor()
                reader = io.BufferedReader(cctx.stream_reader(fh))
                rdr = jsonlines.Reader(reader)
                yield from handle_jsonl(rdr, get_meta, autojoin_paragraphs, para_joiner)
            

    def read_owt(self, file):
        tar = tarfile.open(file, encoding='utf-8')
        utf8reader = codecs.getreader('utf-8')

        for name in tar.getmembers():
            fp = tar.extractfile(name)
            inner_tar = tarfile.open(fileobj=fp, encoding='utf-8')
            for inner_name in inner_tar.getmembers():
                inner_fp = utf8reader(inner_tar.extractfile(inner_name))
                contents = inner_fp.read()
                yield contents

    def read_owt_subset(self, file):
        utf8reader = codecs.getreader('utf-8')
        tar = tarfile.open(file, encoding='utf-8')
        for name in tar.getmembers():
            fp = utf8reader(tar.extractfile(name))
            contents = fp.read()
            yield contents


class Archive:
    def __init__(self, out_dir, compression_level=3):
        self.out_dir = out_dir
        os.makedirs(out_dir, exist_ok=True)
        self.i = 0
        
        self.fh = open(self.out_dir + '/current_chunk_incomplete', 'wb')
        self.cctx = zstandard.ZstdCompressor(level=compression_level)
        self.compressor = self.cctx.stream_writer(self.fh)
        
    
    def add_data(self, data, meta={}):
        self.compressor.write(json.dumps({'text': data, 'meta': meta}).encode('UTF-8') + b'\n')
    
    def commit(self, archive_name='default'):
        fname = self.out_dir + '/data_' + str(self.i) + '_time' + str(int(time.time())) + '_' + archive_name + '.jsonl.zst'
        self.compressor.flush(zstandard.FLUSH_FRAME)
        
        self.fh.flush()
        self.fh.close()
        os.rename(self.out_dir + '/current_chunk_incomplete', fname)
        self.fh = open(self.out_dir + '/current_chunk_incomplete', 'wb')
        self.compressor = self.cctx.stream_writer(self.fh)

        self.i += 1


class DatArchive:
    def __init__(self, out_dir):
        self.out_dir = out_dir
        os.makedirs(out_dir, exist_ok=True)
        self.data = []
        self.i = 0
        if os.path.exists(out_dir) and len(os.listdir(out_dir)) > 0:
            self.i = max(map(lambda x: int(x.split('_')[1].split('.')[0]), os.listdir(out_dir))) + 1
    
    def add_data(self, data):
        self.data.append(data)
    
    def commit(self, archive_name=None):
        # TODO: streaming
        cctx = zstandard.ZstdCompressor(level=3)

        if archive_name is None:
            archive_name = str(int(time.time()))

        res = b''.join(map(lambda x: ("%016d" % len(x)).encode('UTF-8') + x, map(lambda x: x.encode('UTF-8'), self.data)))
        cdata = cctx.compress(res)

        with open(self.out_dir + '/data_' + str(self.i) + '_' + archive_name + '.dat.zst', 'wb') as fh:
            fh.write(cdata)

        self.i += 1
        self.data = []

class JSONArchive:
    def __init__(self, out_dir):
        self.out_dir = out_dir
        os.makedirs(out_dir, exist_ok=True)
        self.data = []
        self.i = 0
        if os.path.exists(out_dir) and len(os.listdir(out_dir)) > 0:
            self.i = max(map(lambda x: int(x.split('_')[1].split('.')[0]), os.listdir(out_dir))) + 1
    
    def add_data(self, data):
        self.data.append(data)
    
    def commit(self):
        cctx = zstandard.ZstdCompressor(level=3)
        
        cdata = cctx.compress(json.dumps(self.data).encode('UTF-8'))
        with open(self.out_dir + '/data_' + str(self.i) + '_' + str(int(time.time())) + '.json.zst', 'wb') as fh:
            fh.write(cdata)

        self.i += 1
        self.data = []

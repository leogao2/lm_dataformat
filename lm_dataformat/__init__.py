import os
import zstandard
import json
import time
import tarfile
import codecs
from functools import reduce
import jsonlines
import io


def listdir_or_file(x):
    if isinstance(x, list):
        return reduce(lambda x,y:x+y, map(listdir_or_file, x))
    return [x] if os.path.isfile(x) else [x + '/' + fn for fn in os.listdir(x)]


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
            elif f.endswith('.json.zst'):
                assert not get_meta

                yield from self.read_json(f)

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
            for ob in rdr:
                text = ob['text']

                if autojoin_paragraphs and isinstance(text, list):
                    text = para_joiner.join(text)

                if get_meta:
                    yield text, (ob['meta'] if 'meta' in ob else {})
                else:
                    yield text

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
        if os.path.exists(out_dir) and len(os.listdir(out_dir)) > 0:
            self.i = max(map(lambda x: int(x.split('_')[1].split('.')[0]), os.listdir(out_dir))) + 1
        
        self.fh = open(self.out_dir + '/current_chunk_incomplete', 'wb')
        self.cctx = zstandard.ZstdCompressor(level=compression_level)
        self.compressor = self.cctx.stream_writer(self.fh)
        
    
    def add_data(self, data, meta={}):
        self.compressor.write(json.dumps({'text': data, 'meta': meta}).encode('UTF-8') + b'\n')
    
    def commit(self, archive_name='default'):
        fname = self.out_dir + '/data_' + str(self.i) + '_time' + str(int(time.time())) + '_' + archive_name + '.jsonl.zst'
        self.compressor.flush(zstandard.FLUSH_FRAME)
        os.rename(self.out_dir + '/current_chunk_incomplete', fname)
        
        self.fh.flush()
        self.fh.close()

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
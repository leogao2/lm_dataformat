import os
import zstandard
import json
import time
import tarfile
import codecs


def listdir_or_file(x):
    return [x] if os.path.isfile(x) else [x + '/' + fn for fn in os.listdir(x)]


class Reader:
    def __init__(self, in_path):
        self.in_path = in_path
    
    def stream_data(self):
            for f in listdir_or_file(self.in_path):
                if f == 'openwebtext.tar.xz':
                    yield from self.read_owt(self.in_path)
                elif f.endswith('.dat.zst'):
                    yield from self.read_dat(f)
                elif f.endswith('.json.zst'):
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


class Archive:
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
        # TODO: streaming
        cctx = zstandard.ZstdCompressor(level=3)

        res = b''.join(map(lambda x: ("%016d" % len(x)).encode('UTF-8') + x, map(lambda x: x.encode('UTF-8'), self.data)))
        cdata = cctx.compress(res)

        with open(self.out_dir + '/data_' + str(self.i) + '_' + str(int(time.time())) + '.dat.zst', 'wb') as fh:
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
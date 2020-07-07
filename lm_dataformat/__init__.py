import os
import zstandard
import json
import time


def listdir(x):
    return [x + '/' + fn for fn in os.listdir(x)]


class Reader:
    def __init__(self, in_dir):
        self.in_dir = in_dir
    
    def stream_data(self):
        for f in listdir(self.in_dir):
            if f.endswith('.dat.zst'):
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
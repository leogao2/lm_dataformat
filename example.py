import lm_dataformat as lmd
import numpy as np
from tqdm import auto as tqdm_lib

def load_jsons(fnames, jsonl_key=None, sample_ratio=1.0):
    def load_json(fname):
        reader = lmd.Reader(fname)
        
        get_meta = True if jsonl_key is None else False # return dict 
        for i, doc in enumerate(tqdm_lib.tqdm(reader.stream_data(jsonl_key=jsonl_key, get_meta=get_meta))):
            if sample_ratio == 1.0 or np.random.rand() < sample_ratio: 
                yield doc 
    
    fnames = [fnames] if not isinstance(fnames, list) else fnames
    for fname in fnames:
        yield from load_json(fname)
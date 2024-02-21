from tqdm.autonotebook import tqdm
from multiprocessing import Pool
from datasketch import MinHash
from typing import Optional
from glob import glob
import pickle
import json
import argparse
from functools import partial

def parse_args():
    parser = argparse.ArgumentParser(description='Generate minhash signatures for a text dataset, given its home directory and data format')

    parser.add_argument('-i', '--input', help='Directory where data is stored', required=True)
    parser.add_argument('-o', '--output', help='Output directory where pickled minhash signatures will be stored', required=True)
    parser.add_argument('-f', '--format', choices=['jsonl'], default='jsonl', help='Data format')
    parser.add_argument('-n', '--num', type=int, default=50000, help='Approximate number of data points in each file (used for multiprocessing)')

    return parser.parse_args()


"""
This allows us to ingest text data and compute minhash signatures from jsonl files.
Each json object may have arbitrary metadata but should store relevant text data for training using the
'text' key. For example, a valid object might look like:

{
    title: 'My Article',
    meta: {pub_date: ...},
    text: {'Some text for training...'}
}
"""
def compute_minhash_jsonl(t: tuple) -> Optional[tuple]:
    lineNo, line = t
    lineNo += 1
    line = json.loads(line)
    line = line.get("text", "")
    s = set(line.split())
    if not s:
        return None
    m = MinHash(num_perm=128)
    for d in s:
        m.update(d.encode('utf8'))
    key = f"{fname}-{lineNo}"
    return (key, m)

def compute_minhash(t: tuple, fmt='jsonl') -> Optional[tuple]:
    if fmt == 'jsonl':
        return compute_minhash_jsonl(t)
    else:
        raise Exception(f"Can not parse unsupported format: {fmt}")


if __name__ == '__main__':
    # get cli arguments
    args = parse_args()
    
    n = args.num    # (roughly) the number of articles stored in each input file
    for infile in glob(f'{args.input}/*.{args.format}'):
        fname = infile.split('/')[-1]
        with open(infile) as fin, Pool(32) as p, tqdm(total=n, desc=fname) as pbar:
            minhash_list = list()
            partial_compute_minhash = partial(compute_minhash, fmt=args.format)
            for result in p.imap_unordered(partial_compute_minhash, enumerate(fin)):
                if result:
                    minhash_list.append(result)
                    pbar.update()            
            with open(f"{args.output}/{fname[:-6]}.pkl", 'wb') as fp:
                pickle.dump(minhash_list, fp)
            print(f"Generated MinHash for {len(minhash_list):,} scientific articles in {fname}")

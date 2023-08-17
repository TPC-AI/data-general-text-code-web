from datasketch import MinHash, MinHashLSH

from tqdm.autonotebook import tqdm
from multiprocessing import Pool
from glob import glob
import argparse
import pickle


def insert(i: int) -> None:
    infile = f'/eagle/tpc/hongz/minhash/prd/sentences_{i:02d}.pkl'
    with open(infile, 'rb') as fin, lsh.insertion_session() as session:
        minhash_list = pickle.load(fin)
        for key, m in tqdm(minhash_list, desc=f"File [{i}]"):
            session.insert(key, m)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodeid", help=f"Idx of the current node", type=str, required=False, default="00")
    args = parser.parse_args()
    nodeid = args.nodeid
    if nodeid == "01":
        port = 16301
        file_idx = range(0, 29)
    elif nodeid == "02":
        port = 16302
        file_idx = range(29, 58)
    elif nodeid == "03":
        port = 16303
        file_idx = range(58, 85)
    else:
        file_idx = range(0, 2)
        port = 16308
    
    basename = b'prd'
    sim_threshold = 0.9
    n_hash_funcs = 128
    lsh = MinHashLSH(
        threshold=sim_threshold, num_perm=n_hash_funcs, storage_config={
            'type': 'redis',
            'basename': basename,
            'redis': {'host': 'localhost', 'port': port},
        }
    )

    with Pool(32) as p:
        p.map(insert, file_idx)

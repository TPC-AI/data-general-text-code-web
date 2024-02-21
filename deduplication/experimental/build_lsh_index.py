from datasketch import MinHash, MinHashLSH

from tqdm.autonotebook import tqdm
from multiprocessing import Pool
from glob import glob
import argparse
import pickle
from functools import partial
import os

def parse_args():
    parser = argparse.ArgumentParser()
    # parser.add_argument("--nodeid", help=f"Idx of the current node", type=str, required=False, default="00")
    parser.add_argument("--input", help="The directory to process", type=str, required=True)
    parser.add_argument("--process_all", action="store_true", help="Whether to process all files in the input directory, if false limit to the range given by start and last file index", required=False, default=False)
    parser.add_argument("--start_file_idx", help="The starting file index to process (inclusive). Default is 0.", type=int, required=False, default=0)
    parser.add_argument("--last_file_idx", help="The last file index (exclusive). Default is 1.", type=int, required=False, default=1)
    parser.add_argument("--redis_port", help="The port that Redis server is listening on. Default is 6379.", type=int, required=False, default=6379)
    parser.add_argument("-n", "--num_processes", help="Number of processes for multiprocessing to use. Default is 32.", type=int, required=False, default=32)
    
    return parser.parse_args()

def insert(infile: str) -> None:
    with open(infile, 'rb') as fin, lsh.insertion_session() as session:
        minhash_list = pickle.load(fin)
        for key, m in tqdm(minhash_list, desc=f"File [{infile}]"):
            session.insert(key, m)


if __name__ == '__main__':
    args = parse_args()

    # nodeid = args.nodeid
    file_idx = range(args.start_file_idx, args.last_file_idx)
    port = args.redis_port
    # if nodeid == "01":
    #     port = 16301
    #     file_idx = range(0, 30)
    # else:
    #     file_idx = range(0, 2)
    #     port = 16308
    
    basename = b'tpc'
    sim_threshold = 0.8
    n_hash_funcs = 128
    lsh = MinHashLSH(
        threshold=sim_threshold, num_perm=n_hash_funcs, storage_config={
            'type': 'redis',
            'basename': basename,
            'redis': {'host': 'localhost', 'port': port},
        }
    )


    pkl_files = [os.path.join(args.input, f) for f in os.listdir(args.input) if f.endswith('.pkl')]
    if not args.process_all:
        pkl_files = pkl_files[args.start_file_idx : args.last_file_idx]

    with Pool(args.num_processes) as p:
        p.map(insert, pkl_files)

from datasketch import MinHash, MinHashLSH

from tqdm.autonotebook import tqdm
from multiprocessing import Pool
from glob import glob
import argparse
import pickle


def insert(i: int) -> None:
    infile = f'/eagle/tpc/hongz/minhash/pile/sci_{i:02d}.pkl'
    with open(infile, 'rb') as fin, lsh.insertion_session() as session:
        minhash_list = pickle.load(fin)
        for key, m in tqdm(minhash_list, desc=f"File [{i}]"):
            session.insert(key, m)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument("--nodeid", help=f"Idx of the current node", type=str, required=False, default="00")
    parser.add_argument("--start_file_idx", help="The starting file index to process (inclusive). Default is 0.", type=int, required=False, default=0)
    parser.add_argument("--last_file_idx", help="The last file index (exclusive). Default is 1.", type=int, required=False, default=1)
    parser.add_argument("--redis_port", help="The port that Redis server is listening on. Default is 6379.", type=int, required=False, default=6379)
    args = parser.parse_args()
    # nodeid = args.nodeid
    file_idx = range(args.start_file_idx, args.last_file_idx)
    port = args.port
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

    with Pool(32) as p:
        p.map(insert, file_idx)

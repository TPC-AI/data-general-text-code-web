from datasketch import MinHash, MinHashLSH
from tqdm.autonotebook import tqdm
from multiprocessing import Pool
from glob import glob
import argparse
import pickle
import json
import csv
import time

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help=f"Input directory with precomputed minhash signatures", type=str, required=True)
    parser.add_argument("--output", help=f"Output csv file path", type=str, required=True)
    parser.add_argument("--nodeid", help=f"Idx of the current node", type=str, required=False, default="00")
    parser.add_argument("--redis_port", help=f"Port for local Redis server. Default is 6379.", type=int, required=False, default=6379)
    parser.add_argument("--profile-fp", help=f"Whether to profile false positives. This will slow down execution and should only be used for profiling.", action="store_true")
    
    return parser.parse_args()

def query(params: tuple) -> list:
    key, m_query = params
    result = lsh.query(m_query)
    
    # profile false positive rate
    if args.profile_fp:
        for dup_key in result:
            print("DUPKEY:", lsh.keys.get(dup_key))

    return [(key, dup_key) for dup_key in result]


if __name__ == '__main__':
    start_program = time.perf_counter()

    args = parse_args()

    nodeid = args.nodeid
    # if nodeid == "01":
    #     port = 16301
    # else:
    #     port = 16308

    port = args.redis_port

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
    # outfile = f'/grand/SuperBERT/hongz/PRDvsPILE/duplicates_sci_{nodeid}.csv'
    # outfile = f'/grand/SuperBERT/hongz/PRDvsPILE/duplicates{nodeid}.jsonl'
    outfile = args.output
    indir = args.input
    
    start_files = time.perf_counter()
    avg_time_per_file = 0
    n = 0
    with open(outfile, 'w') as fout:
        writer = csv.writer(fout)
        for minhashfile in glob(f'{indir}/*.pkl'):
            s_file = time.perf_counter()
            with open(minhashfile, 'rb') as fin:
                minhash_list = pickle.load(fin)
                fname = minhashfile.split('/')[-1]
                with Pool(32) as p, tqdm(total=len(minhash_list), desc=fname) as pbar:
                    for result in p.imap_unordered(query, minhash_list):
                        if result:
                            writer.writerows(result)
                        pbar.update()
            
            elapsed_file = time.perf_counter() - s_file
            avg_time_per_file += elapsed_file
            n += 1

    avg_time_per_file /= n
    elapsed_proc_files = time.perf_counter() - start_files
    elapsed_program = time.perf_counter() - start_program

    print("\n")
    print(f"Average time to query pickle file: {avg_time_per_file} seconds")
    print(f"Time to query all {n} pickle files: {elapsed_proc_files} seconds")
    print(f"Total program runtime: {elapsed_program} seconds")
    print("\n")
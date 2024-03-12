from datasketch import MinHash, MinHashLSH
from tqdm.autonotebook import tqdm
from multiprocessing import Pool
from glob import glob
import argparse
import pickle
import json
import csv
import time
import os

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help=f"Input directory with precomputed minhash signatures", type=str, required=True)
    parser.add_argument("--output", help=f"Output csv file path", type=str, required=True)
    parser.add_argument('-t', '--tag', help='Tag, used when logging profiling information (e.g. rp1_vs_arxiv)', required=True)
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
    file_sizes = 0
    num_articles = 0
    n = 0
    with open(outfile, 'w') as fout:
        writer = csv.writer(fout)
        for minhashfile in glob(f'{indir}/*.pkl'):
            s_file = time.perf_counter()
            file_sizes += os.path.getsize(minhashfile)
            with open(minhashfile, 'rb') as fin:
                minhash_list = pickle.load(fin)
                fname = minhashfile.split('/')[-1]
                with Pool(32) as p, tqdm(total=len(minhash_list), desc=fname) as pbar:
                    for result in p.imap_unordered(query, minhash_list):
                        num_articles += 1
                        if result:
                            writer.writerows(result)
                        pbar.update()
            
            elapsed_file = time.perf_counter() - s_file
            avg_time_per_file += elapsed_file
            n += 1

    avg_time_per_file /= n
    avg_minhash_file_size = file_sizes / n
    avg_num_articles_per_file = num_articles / n
    elapsed_proc_files = time.perf_counter() - start_files

    print("\n")
    print(f"Average time to query pickle file: {avg_time_per_file} seconds")
    print(f"Time to query all {n} pickle files: {elapsed_proc_files} seconds")
    print(f"Average pickle file size: {avg_minhash_file_size}")
    print(f"Total pickle file size: {file_sizes}")
    print(f"Average num articles per file: {avg_num_articles_per_file}")
    print(f"Total num articles: {num_articles}")
    print("\n")


    headers=['id', 'avg_time_per_file_sec', 'total_time_sec', 'avg_size_input_bytes', 'total_size_input_bytes', 
        'avg_num_documents_per_file', 'num_documents', 'num_input_files']
    fname = 'query_lsh_index_profile.csv'

    data = [args.tag, avg_time_per_file, elapsed_proc_files, avg_minhash_file_size, file_sizes,
            avg_num_articles_per_file, num_articles, n]
    with open(fname, 'a+') as f:
        writer = csv.writer(f)
        # if file is empty write header row
        if os.stat(fname).st_size == 0:
            writer.writerow(headers)
        
        writer.writerow(data)
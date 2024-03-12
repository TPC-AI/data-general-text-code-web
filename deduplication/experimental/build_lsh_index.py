from datasketch import MinHash, MinHashLSH

from tqdm.autonotebook import tqdm
from multiprocessing import Pool
from glob import glob
import argparse
import pickle
from functools import partial
import os
import time
import csv


def parse_args():
    parser = argparse.ArgumentParser()
    # parser.add_argument("--nodeid", help=f"Idx of the current node", type=str, required=False, default="00")
    parser.add_argument(
        "--input", help="The directory to process", type=str, required=True
    )
    parser.add_argument(
        "-t",
        "--tag",
        help="Tag, used when logging profiling information (e.g. rp1_arxiv)",
        required=True,
    )
    parser.add_argument(
        "--process_all",
        action="store_true",
        help="Whether to process all files in the input directory, if false limit to the range given by start and last file index",
        required=False,
        default=False,
    )
    parser.add_argument(
        "--start_file_idx",
        help="The starting file index to process (inclusive). Default is 0.",
        type=int,
        required=False,
        default=0,
    )
    parser.add_argument(
        "--last_file_idx",
        help="The last file index (exclusive). Default is 1.",
        type=int,
        required=False,
        default=1,
    )
    parser.add_argument(
        "--redis_port",
        help="The port that Redis server is listening on. Default is 6379.",
        type=int,
        required=False,
        default=6379,
    )
    parser.add_argument(
        "-n",
        "--num_processes",
        help="Number of processes for multiprocessing to use. Default is 32.",
        type=int,
        required=False,
        default=32,
    )

    return parser.parse_args()


def insert(infile: str) -> None:
    n = 0
    with open(infile, "rb") as fin:  # , lsh.insertion_session() as session:
        print("in session")
        minhash_list = pickle.load(fin)
        print("loaded minhashes")
        for key, m in tqdm(minhash_list, desc=f"File [{infile}]"):
            result = lsh.query(m)

            # insert if not duplicated in index
            if not len(result) or (len(result) == 1 and result[0] == key):
                lsh.insert(key, m)
                n += 1
            # session.insert(key, m)

    return n


# def insert(t) -> None:
#     n = 0
#     key, m = t

#     result = lsh.query(m)

#     # insert if not duplicated in index
#     if not len(result) or (len(result) == 1 and result[0] == key):
#         lsh.insert(key, m)
#         n += 1

#     return n

if __name__ == "__main__":
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
    print("start")

    basename = b"tpc"
    sim_threshold = 0.8
    n_hash_funcs = 128
    lsh = MinHashLSH(
        threshold=sim_threshold,
        num_perm=n_hash_funcs,
        storage_config={
            "type": "redis",
            "basename": basename,
            "redis": {"host": "localhost", "port": port},
        },
    )

    print("lsh init")

    pkl_files = [
        os.path.join(args.input, f)
        for f in os.listdir(args.input)
        if f.endswith(".pkl")
    ]
    if not args.process_all:
        pkl_files = pkl_files[args.start_file_idx : args.last_file_idx]

    n_files = len(pkl_files)
    print(n_files)
    # print(pkl_files)
    start = time.perf_counter()
    file_sizes = sum([os.path.getsize(f) for f in pkl_files])
    avg_file_size = file_sizes / n_files

    num_docs = 0
    # with Pool(args.num_processes) as p:
    #      for infile in pkl_files:
    #         fname = infile.split('/')[-1]
    #         with open(infile, 'rb') as fin, tqdm(total=50000, desc=fname) as pbar:
    #             minhash_list = pickle.load(fin)
    #             for result in p.map(insert, minhash_list):
    #                 num_docs += result
    #                 pbar.update()

    for f in pkl_files:
        num_docs += insert(f)
        print("complete")

    print("NUM DOCS: ", num_docs)

    print("ffin")
    elapsed_proc_seconds = time.perf_counter() - start
    avg_time_per_file = elapsed_proc_seconds / n_files

    print("\n")
    print(f"Average time to query pickle file: {avg_time_per_file} seconds")
    print(f"Time to query all {n_files} pickle files: {elapsed_proc_seconds} seconds")
    print(f"Average pickle file size: {avg_file_size}")
    print(f"Total pickle file size: {file_sizes}")
    print("\n")

    headers = [
        "id",
        "avg_time_per_file_sec",
        "total_time_sec",
        "avg_size_input_bytes",
        "total_size_input_bytes",
        "num_input_files",
    ]
    fname = "build_lsh_index_profile.csv"

    data = [
        args.tag,
        avg_time_per_file,
        elapsed_proc_seconds,
        avg_file_size,
        file_sizes,
        n_files,
    ]
    with open(fname, "a+") as f:
        writer = csv.writer(f)
        # if file is empty write header row
        if os.stat(fname).st_size == 0:
            writer.writerow(headers)

        writer.writerow(data)

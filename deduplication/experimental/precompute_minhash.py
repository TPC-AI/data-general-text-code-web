from tqdm.autonotebook import tqdm
from multiprocessing import Pool
from datasketch import MinHash
from typing import Optional
from glob import glob
import pickle
import json
import argparse
import time
import os
from functools import partial
import csv


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate minhash signatures for a text dataset, given its home directory and data format"
    )

    parser.add_argument(
        "-i", "--input", help="Directory where data is stored", required=True
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output directory where pickled minhash signatures will be stored",
        required=True,
    )
    parser.add_argument(
        "-t",
        "--tag",
        help="Name of corpus, used when logging profiling information",
        required=True,
    )
    parser.add_argument(
        "-f", "--format", choices=["jsonl", "json"], default="jsonl", help="Data format"
    )
    parser.add_argument(
        "-n",
        "--num",
        type=int,
        default=50000,
        help="Approximate number of data points in each file (used for multiprocessing)",
    )

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
        m.update(d.encode("utf8"))
    key = f"{fname}-{lineNo}"
    return (key, m)


def compute_minhash(t: tuple, fmt="jsonl") -> Optional[tuple]:
    if fmt == "jsonl" or fmt == "json":
        return compute_minhash_jsonl(t)
    else:
        raise Exception(f"Can not parse unsupported format: {fmt}")


if __name__ == "__main__":
    # get cli arguments
    args = parse_args()
    os.makedirs(args.output, exist_ok=True)

    n_files = 0
    file_size = 0
    start = time.perf_counter()
    avg_time = 0
    num_articles = 0
    n = args.num  # (roughly) the number of articles stored in each input file
    for infile in glob(f"{args.input}/*.{args.format}"):
        fname = infile.split("/")[-1]
        n_files += 1
        file_size += os.path.getsize(infile)
        with open(infile) as fin, Pool(32) as p, tqdm(total=n, desc=fname) as pbar:
            minhash_list = list()
            partial_compute_minhash = partial(compute_minhash, fmt=args.format)
            s = time.perf_counter()
            for result in p.imap_unordered(partial_compute_minhash, enumerate(fin)):
                num_articles += 1
                if result:
                    minhash_list.append(result)
                    pbar.update()
            with open(f"{args.output}/{fname[:-6]}.pkl", "wb") as fp:
                pickle.dump(minhash_list, fp)
            print(
                f"Generated MinHash for {len(minhash_list):,} scientific articles in {fname}"
            )

            avg_time += time.perf_counter() - s

    elapsed_program = time.perf_counter() - start
    avg_time /= n_files
    avg_file_size = file_size / n_files
    avg_num_articles_per_file = num_articles / n_files

    print("\n")
    print(f"Average time to process file: {avg_time} seconds")
    print(f"Total program runtime: {elapsed_program} seconds")
    print(f"Total size of jsonl files: {file_size} bytes")
    print(f"{n_files} jsonl files processed, average size: {avg_file_size}")
    print(f"{num_articles} number of total articles processed")
    print(f"Average number of articles per jsonl file: {avg_num_articles_per_file}")
    print("\n")

    # compute size of all minhash signature files
    mh_size = 0
    n_pkl = 0
    for mhfile in glob(f"{args.output}/*.pkl"):
        mh_size += os.path.getsize(mhfile)
        n_pkl += 1

    avg_mh_size = mh_size / n_pkl
    print(f"Total size of generated minhash signatures: {mh_size} bytes")
    print(f"{n_pkl} pickle files generated, average size: {avg_mh_size}")
    print("\n")

    headers = [
        "corpus",
        "avg_time_per_file_sec",
        "total_time_sec",
        "avg_size_input_bytes",
        "total_size_input_bytes",
        "avg_size_output_bytes",
        "total_size_output_bytes",
        "avg_num_documents_per_file",
        "num_documents",
        "num_input_files",
    ]
    fname = "minhash_profile.csv"

    data = [
        args.tag,
        avg_time,
        elapsed_program,
        avg_file_size,
        file_size,
        avg_mh_size,
        mh_size,
        avg_num_articles_per_file,
        num_articles,
        n_files,
    ]
    with open(fname, "a+") as f:
        writer = csv.writer(f)
        # if file is empty write header row
        if os.stat(fname).st_size == 0:
            writer.writerow(headers)

        writer.writerow(data)

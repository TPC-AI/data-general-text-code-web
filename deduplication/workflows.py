from ingest import *
from writers import write_duplicates_to_csv
from typing import List
from glob import glob


# workflow for deduping single corpus against the LSH Index
def dedup_single(
    input_dir: str,
    minhash_dir: str,
    csvfile: str,
    sim_threshold: float = 0.8,
    n_hash_funcs: int = 128,
    redis_name: str = b"tpc",
    redis_port: int = 6379,
    compute_minhashes: bool = True,
):
    lsh_params = {
        "threshold": sim_threshold,
        "num_perm": n_hash_funcs,
        "storage_config": {
            "type": "redis",
            "basename": redis_name,
            "redis": {"host": "localhost", "port": redis_port},
        },
    }

    if compute_minhashes:
        compute_minhash_signatures(input_dir, minhash_dir)

    duplicates = deduplicate_corpus(glob(f"{minhash_dir}/*.pkl"), lsh_params)
    write_duplicates_to_csv(duplicates, csvfile)


# workflow for deduping many corpora at once
def dedup_multi(
    input_dirs: List[str],
    minhash_dirs: List[str],
    csvfile: str,
    sim_threshold: float = 0.8,
    n_hash_funcs: int = 128,
    redis_name: str = b"tpc",
    redis_port: int = 6379,
    compute_minhashes: bool = True,
):
    assert len(input_dirs) == len(
        minhash_dirs
    ), "Expected the same number of input directories to output minhash directories"

    for i in range(len(input_dirs)):
        dedup_single(
            input_dirs[i],
            minhash_dirs[i],
            csvfile,
            sim_threshold,
            n_hash_funcs,
            redis_name,
            redis_port,
            compute_minhashes,
        )


def dedup_single_file(
    input_file: str,
    minhash_dir: str,
    csvfile: str,
    sim_threshold: float = 0.8,
    n_hash_funcs: int = 128,
    redis_name: str = b"tpc",
    redis_port: int = 6379,
    compute_minhashes: bool = True,
):
    lsh_params = {
        "threshold": sim_threshold,
        "num_perm": n_hash_funcs,
        "storage_config": {
            "type": "redis",
            "basename": redis_name,
            "redis": {"host": "localhost", "port": redis_port},
        },
    }

    if compute_minhashes:
        compute_minhash_signatures_file(input_file, minhash_dir)

    fname = input_file.split("/")[-1]
    minhash_file = f"{minhash_dir}/{fname[:-6]}.pkl"
    duplicates = deduplicate_corpus([minhash_file], lsh_params)
    write_duplicates_to_csv(duplicates, csvfile)

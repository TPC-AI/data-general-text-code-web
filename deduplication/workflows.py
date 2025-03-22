from deduplication.minhash import MinHasher
from deduplication.lsh import LSHIndex
from deduplication.lshbloom import LSHBloom
from deduplication.writers import write_duplicates_to_csv
from typing import List
import os

# <<< MinHashLSH >>>

# workflow for deduping single corpus against the LSH Index
def dedup_single_lsh(
    input_dir: str,
    minhash_dir: str,
    csvfile: str,
    corpus_name: str,
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
        m = MinHasher(input_dir, minhash_dir, n_hash_funcs)
        m.process()

    index = LSHIndex(minhash_dir, lsh_params)
    duplicates = index.deduplicate_corpus()
    write_duplicates_to_csv(duplicates, csvfile, corpus_name, header=["corpus", "key", "dup_key"])


# workflow for deduping many corpora at once
def dedup_multi_lsh(
    input_dirs: List[str],
    minhash_dirs: List[str],
    csvfile: str,
    corpus_names: List[str],
    sim_threshold: float = 0.8,
    n_hash_funcs: int = 128,
    redis_name: str = b"tpc",
    redis_port: int = 6379,
    compute_minhashes: bool = True,
):
    assert len(input_dirs) == len(minhash_dirs) == len(corpus_names), \
        f"Expected len(input_dirs) == len(minhash_dirs) == len(corpus_names), got {len(input_dirs)}, {len(minhash_dirs)}, {len(corpus_names)}"

    for i in range(len(input_dirs)):
        dedup_single_lsh(
            input_dirs[i],
            minhash_dirs[i],
            csvfile,
            corpus_names[i],
            sim_threshold,
            n_hash_funcs,
            redis_name,
            redis_port,
            compute_minhashes,
        )


def dedup_single_file_lsh(
    input_file: str,
    minhash_dir: str,
    csvfile: str,
    corpus_name: str,
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
        m = MinHasher(None, minhash_dir, n_hash_funcs)
        m.compute_minhash_for_file(input_file)

    fname = input_file.split("/")[-1]
    minhash_file = f"{minhash_dir}/{fname[:-6]}.pkl"
    index = LSHIndex(minhash_dir, lsh_params)
    duplicates = index.deduplicate_minhash_file(minhash_file)
    write_duplicates_to_csv(duplicates, csvfile, corpus_name, header=["key", "dup_key"])


# <<< LSHBloom >>>

def clear_dir(save_dir):
    if os.path.exists(save_dir):
        rm_files = [os.path.join(save_dir, f) for f in os.listdir(save_dir) if ".bf" in f or '.csv' in f]
        for f in rm_files:
            os.remove(f)


# workflow for deduping single corpus against the Bloom Index
def dedup_single_bloom(
    input_dir: str,
    minhash_dir: str,
    corpus_size: int,
    false_positive_rate: float,
    csvfile: str,
    corpus_name: str,
    sim_threshold: float = 0.8,
    n_hash_funcs: int = 128,
    save_dir: str = "./",
    compute_minhashes: bool = True,
    clear: bool = False,
):
    if clear:
        clear_dir(save_dir)
    
    lsh_params = {
        "threshold": sim_threshold,
        "num_perm": n_hash_funcs,
        "n": corpus_size,
        "fp": false_positive_rate,
        "save_dir": save_dir
    }

    if compute_minhashes:
        m = MinHasher(input_dir, minhash_dir, n_hash_funcs)
        m.process()

    index = LSHBloom(minhash_dir, lsh_params)
    duplicates = index.deduplicate_corpus()
    write_duplicates_to_csv(duplicates, csvfile, corpus_name, header=["dup_key"])


# workflow for deduping many corpora against the Bloom Index at once
def dedup_multi_bloom(
    input_dirs: List[str],
    minhash_dirs: List[str],
    corpus_size: int,
    false_positive_rate: float,
    csvfile: str,
    corpus_names: List[str],
    sim_threshold: float = 0.8,
    n_hash_funcs: int = 128,
    save_dir: str = "./",
    compute_minhashes: bool = True,
    clear: bool = False,
):
    assert len(input_dirs) == len(minhash_dirs) == len(corpus_names), \
        f"Expected len(input_dirs) == len(minhash_dirs) == len(corpus_names), got {len(input_dirs)}, {len(minhash_dirs)}, {len(corpus_names)}"

    if clear:
        clear_dir(save_dir)

    for i in range(len(input_dirs)):
        dedup_single_bloom(
            input_dirs[i],
            minhash_dirs[i],
            corpus_size,
            false_positive_rate,
            csvfile,
            corpus_names[i],
            sim_threshold,
            n_hash_funcs,
            save_dir,
            compute_minhashes,
            clear=False
        )

def dedup_single_file_bloom(
    input_file: str,
    minhash_dir: str,
    corpus_size: int,
    false_positive_rate: float,
    csvfile: str,
    corpus_name: str,
    sim_threshold: float = 0.8,
    n_hash_funcs: int = 128,
    save_dir: str = "./",
    compute_minhashes: bool = True,
    clear: bool = False,
):
    if clear:
        clear_dir(save_dir)

    lsh_params = {
        "threshold": sim_threshold,
        "num_perm": n_hash_funcs,
        "n": corpus_size,
        "fp": false_positive_rate,
        "save_dir": save_dir
    }

    if compute_minhashes:
        m = MinHasher(None, minhash_dir, n_hash_funcs)
        m.compute_minhash_for_file(input_file)

    fname = input_file.split("/")[-1]
    minhash_file = f"{minhash_dir}/{fname[:-6]}.pkl"
    index = LSHBloom(minhash_dir, lsh_params)
    duplicates = index.deduplicate_minhash_file(minhash_file)
    write_duplicates_to_csv(duplicates, csvfile, corpus_name, header=["dup_key"])

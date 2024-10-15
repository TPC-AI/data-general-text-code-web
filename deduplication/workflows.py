import os
from functools import partial
from pathlib import Path
from typing import List

from parsl.concurrent import ParslPoolExecutor

from deduplication.lsh import LSHIndex
from deduplication.lshbloom import LSHBloom, run_LSHBloom
from deduplication.minhash import MinHasher
from deduplication.writers import write_duplicates_to_csv

from .minhash import compute_minhash_for_file
from .parsl_conf import ComputeSettingsTypes

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
    write_duplicates_to_csv(
        duplicates, csvfile, corpus_name, header=["corpus", "key", "dup_key"]
    )


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
    assert (
        len(input_dirs) == len(minhash_dirs) == len(corpus_names)
    ), f"Expected len(input_dirs) == len(minhash_dirs) == len(corpus_names), got {len(input_dirs)}, {len(minhash_dirs)}, {len(corpus_names)}"

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
        rm_files = [
            os.path.join(save_dir, f) for f in save_dir if ".bf" in f or ".csv" in f
        ]
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
    parsl_config: ComputeSettingsTypes = None,
    num_cpus: int = 32,
):
    if clear:
        clear_dir(save_dir)

    lsh_params = {
        "threshold": sim_threshold,
        "num_perm": n_hash_funcs,
        "n": corpus_size,
        "fp": false_positive_rate,
        "save_dir": save_dir,
    }

    if parsl_config is None:
        # Compute minhashes if needed
        if compute_minhashes:
            m = MinHasher(input_dir, minhash_dir, n_hash_funcs, num_cpus)
            m.process()

        # Deduplicate with Bloom Index
        index = LSHBloom(minhash_dir, lsh_params)
        duplicates = index.deduplicate_corpus()
        write_duplicates_to_csv(duplicates, csvfile, corpus_name, header=["dup_key"])

        return

    elif parsl_config is not None:
        config = parsl_config.get_config()

        with ParslPoolExecutor(config) as executor:
            print(f"Parsl config: {parsl_config}")
            if compute_minhashes:
                print("Using Parsl to compute minhashes")

                minhash_func = partial(
                    compute_minhash_for_file,
                    output_dir=minhash_dir,
                    num_perm=n_hash_funcs,
                    num_cpus=parsl_config.cores_per_worker,
                    progress=False,
                )
                Path(minhash_dir).mkdir(parents=True, exist_ok=True)
                jsonl_files = list(Path(input_dir).glob("*.jsonl"))
                print(f"Found {len(jsonl_files)} jsonl files to process")

                # Compute minhashes in parallel
                list(executor.map(minhash_func, jsonl_files))
                print("Finished computing minhashes for all jsonl files")

            print("Running LSH Bloom deduplication")
            bloom_func = partial(
                run_LSHBloom,
                lsh_params=lsh_params,
                csvfile=csvfile,
                corpus_name=corpus_name,
            )
            executor.submit(bloom_func, minhash_dir).result()
            print(f"Written duplicates to: {csvfile}")

        return


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
    parsl_config: ComputeSettingsTypes = None,
):
    assert (
        len(input_dirs) == len(minhash_dirs) == len(corpus_names)
    ), f"Expected len(input_dirs) == len(minhash_dirs) == len(corpus_names), got {len(input_dirs)}, {len(minhash_dirs)}, {len(corpus_names)}"

    if clear:
        clear_dir(save_dir)

    if parsl_config is None:
        # Compute in serial
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
                clear=False,
            )

    elif parsl_config is not None:
        # Need this function to alter the order of arguments to match
        # what changes between each call
        def dedup_single_wrapper(input_dir, minhash_dir, corpus_name, **kwargs):
            # Call the original function with the correct mapping of arguments
            dedup_single_bloom(
                input_dir=input_dir,
                minhash_dir=minhash_dir,
                corpus_name=corpus_name,
                **kwargs,  # Pass the remaining fixed arguments
            )

        # Setup parsl config
        config = parsl_config.get_config()
        print(f"Parsl config: {parsl_config}")

        process_func = partial(
            dedup_single_wrapper,
            corpus_size=corpus_size,
            false_positive_rate=false_positive_rate,
            csvfile=csvfile,
            sim_threshold=sim_threshold,
            n_hash_funcs=n_hash_funcs,
            save_dir=save_dir,
            compute_minhashes=compute_minhashes,
            clear=False,
            parsl_config=None,  # We don't want to use parsl within parsl
            num_cpus=parsl_config.cores_per_worker,
        )
        print("Running LSH Bloom deduplication")
        with ParslPoolExecutor(config) as executor:
            list(executor.map(process_func, input_dirs, minhash_dirs, corpus_names))


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
    parsl_config: dict = None,
):
    if clear:
        clear_dir(save_dir)

    lsh_params = {
        "threshold": sim_threshold,
        "num_perm": n_hash_funcs,
        "n": corpus_size,
        "fp": false_positive_rate,
        "save_dir": save_dir,
    }

    if compute_minhashes:
        m = MinHasher(None, minhash_dir, n_hash_funcs)
        m.compute_minhash_for_file(input_file)

    fname = input_file.split("/")[-1]
    minhash_file = f"{minhash_dir}/{fname[:-6]}.pkl"
    index = LSHBloom(minhash_dir, lsh_params)
    duplicates = index.deduplicate_minhash_file(minhash_file)
    write_duplicates_to_csv(duplicates, csvfile, corpus_name, header=["dup_key"])

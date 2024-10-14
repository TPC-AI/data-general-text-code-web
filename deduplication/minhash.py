import json
import os
import pickle
from functools import partial
from glob import glob
from multiprocessing import Pool
from typing import Optional

from datasketch import MinHash
from tqdm.autonotebook import tqdm

# TODO check if minhashes already exist, recompute only if forced


def compute_minhash_jsonl(t, fname, num_perm):
    lineNo, line = t
    lineNo += 1
    line = json.loads(line)
    line = line.get("text", "")
    s = set(line.split())
    if not s:
        return None
    m = MinHash(num_perm=num_perm)
    for d in s:
        m.update(d.encode("utf8"))
    # generate a unique key for this document
    key = f"{fname}-{lineNo}"
    return (key, m)


def compute_minhash_for_file(
    infile: str,
    output_dir: str,
    num_perm: int,
    num_cpus: int = 32,
    progress: bool = True,
):
    """
    Compute minhash signatures for a given jsonl file with the format specified for
    'compute_minhash_jsonl' above.

    infile is the path to the singular jsonl file
    will store the minhash signatures in self.output_dir
    """
    n = 50000
    fname = str(infile).split("/")[-1]

    if progress:
        pbar = tqdm(total=n, desc=fname)
    else:
        pbar = None

    with open(infile) as fin, Pool(num_cpus) as p:
        minhash_list = list()
        partial_compute_minhash = partial(
            compute_minhash_jsonl, fname=fname, num_perm=num_perm
        )
        for result in p.imap_unordered(partial_compute_minhash, enumerate(fin)):
            if result:
                minhash_list.append(result)
                if pbar:
                    pbar.update()
        with open(f"{output_dir}/{fname[:-6]}.pkl", "wb") as fp:
            pickle.dump(minhash_list, fp)
        print(f"Generated MinHash for {len(minhash_list):,} documents in {fname}")


class MinHasher:
    """
    Handles computing minhash signatures using datasketch

    Example usage:
    ```
    indir = "/data/jsonl_data/"
    outdir = "/data/minhashes/"
    m = MinHasher(indir, outdir)
    m.process() # signatures will be stored in outdir
    ```
    """

    def __init__(
        self, jsonl_dir: str, output_dir: str, num_perm: int = 128, num_cpus: int = 32
    ):
        """
        jsonl_dir: path to jsonl files for the given corpus
        output_dir: path to save minhash signatures to for the given corpus
        """
        self.input_dir = jsonl_dir
        self.output_dir = output_dir
        self.num_perm = num_perm
        self.num_cpus = num_cpus

        os.makedirs(self.output_dir, exist_ok=True)

    def process(self):
        """
        Compute minhash signatures for a directory of jsonl files with the format specified for
        'self.compute_minhash_jsonl'.
        """
        for infile in glob(f"{self.input_dir}/*.jsonl"):
            self.compute_minhash_for_file(infile)

    def compute_minhash_jsonl(self, t: tuple, fname: str) -> Optional[tuple]:
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
        return compute_minhash_jsonl(t, fname, self.num_perm)

    def compute_minhash_for_file(self, infile: str):
        """
        Compute minhash signatures for a given jsonl file with the format specified for
        'compute_minhash_jsonl' above.

        infile is the path to the singular jsonl file
        will store the minhash signatures in self.output_dir
        """
        compute_minhash_for_file(infile, self.output_dir, self.num_perm, self.num_cpus)

from tqdm.autonotebook import tqdm
from multiprocessing import Pool
from datasketch import MinHash, MinHashLSHBloom
from typing import Optional, List, Tuple, Dict
from glob import glob
import pickle
import json
from functools import partial
import os

class LSHBloom:
    """
    Constructs a MinHashLSH Index using datasketch with Bloom Filters as a backend

    Example Usage:
    ```
    # minhash signatures are computed or already exist in minhashdir
    m = MinHasher(indir, minhashdir)
	m.process()

    lsh_params = {...}
    index = LSHBloom(minhashdir, lsh_params)
    index.deduplicate_corpus() # creates index and stores based on lsh_params
    ```
    """
    def __init__(self, minhash_dir: str, lsh_params: Dict):
        """
        minhash_dir: path to directory of pickled minhash signatures
        lsh_params: dict of parameters for MinHashLSH for datasketch

        for more info on how to set lsh_params see here: https://github.com/123epsilon/datasketch/blob/lsh_bloom/datasketch/lsh_bloom.py#L95
        """
        self.minhash_dir = minhash_dir
        self.lsh = MinHashLSHBloom(**lsh_params)

    def deduplicate_corpus(self) -> List[str]:
        """
        Deduplicates documents in the given corpus and adds them to the LSH index if appropriate.
        Documents without existing duplicates will be stored in the LSH index for future deduplication.

        returns a list of document keys representing duplicated documents
        """
        duplicate_list = []
        minhash_files = [
            os.path.join(self.minhash_dir, f)
            for f in os.listdir(self.minhash_dir)
            if f.endswith(".pkl")
        ]
        for minhashfile in minhash_files:
            dups = self.deduplicate_minhash_file(minhashfile)
            duplicate_list.extend(dups)

        return duplicate_list

    def deduplicate_and_insert(self, params: Tuple) -> List[str]:
        """
        Deduplicates a MinHash signature corresponding to a document using the provided LSH index.
        If the document is not duplicated in the LSH index, it is added to the index.

        params - tuple with two elements (key, minhash) where the key uniquely identifies the current document
        and minhash is the document's minhash signature

        returns a list of tuples of the form (key, dup_key) that identify which documents
        from the LSH index have been matched as duplicates with respect to the current document
        """
        # query against lsh index
        key, m_query = params
        result = self.lsh.query(m_query)

        # insert if not duplicated in index
        if not result:
            self.lsh.insert(m_query)

        return [key]

    def deduplicate_minhash_file(self, minhashfile: str) -> List[str]:
        """
        Deduplicate documents in the given minhash file and adds them to the LSH index if appropriate.
        Documents without existing duplicates will be stored in the LSH index for future deduplication.

        minhashfile - path to file of minhash signatures stored in pickle format

        returns a list of tuples of the form (key, dup_key) representing duplicated documents,
        key is from the corpus we are currently considering and dup_key is from the LSH index.

        Note: currently, this should only be run through deduplicate_corpus in order to ensure instantiation of the lsh object
        """
        duplicate_list = []
        with open(minhashfile, "rb") as fin:
            minhash_list = pickle.load(fin)
            fname = minhashfile.split("/")[-1]
            with Pool(32) as p, tqdm(total=len(minhash_list), desc=fname) as pbar:
                for result in p.imap_unordered(self.deduplicate_and_insert, minhash_list):
                    if result:
                        duplicate_list.extend(result)
                    pbar.update()

        return duplicate_list


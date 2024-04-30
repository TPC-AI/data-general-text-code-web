from tqdm.autonotebook import tqdm
from multiprocessing import Pool
from datasketch import MinHashLSH
from typing import List, Tuple, Dict
import pickle
import os

class LSHIndex:
    """
    Constructs a MinHashLSH Index using datasketch

    Example Usage:
    ```
    # minhash signatures are computed or already exist in minhashdir
    m = MinHasher(indir, minhashdir)
	m.process()

    lsh_params = {...}
    index = LSHIndex(minhashdir, lsh_params)
    duplicates = index.deduplicate_corpus() # creates index and stores based on lsh_params
    ```
    """
    def __init__(self, minhash_dir: str, lsh_params: Dict):
        """
        minhash_dir: path to directory of pickled minhash signatures
        lsh_params: dict of parameters for MinHashLSH for datasketch

        for more info on how to set lsh_params see here: https://ekzhu.com/datasketch/documentation.html#minhash-lsh
        """
        self.minhash_dir = minhash_dir
        self.lsh = MinHashLSH(**lsh_params)

    def deduplicate_corpus(self) -> List[Tuple[str]]:
        """
        Deduplicates documents in the given corpus and adds them to the LSH index if appropriate.
        Documents without existing duplicates will be stored in the LSH index for future deduplication.

        returns a list of tuples of the form (key, dup_key) representing duplicated documents,
        key is from the corpus we are currently considering and dup_key is from the LSH index.
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

    def deduplicate_and_insert(self, params: Tuple) -> List[Tuple[str]]:
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
        if not len(result) or (len(result) == 1 and result[0] == key):
            self.lsh.insert(key, m_query)

        return [(key, dup_key) for dup_key in result]

    def deduplicate_minhash_file(self, minhashfile: str) -> List[Tuple[str]]:
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
            with tqdm(total=len(minhash_list), desc=fname) as pbar:
                for i in range(len(minhash_list)):
                    result = self.deduplicate_and_insert(minhash_list[i])
                    if result:
                        duplicate_list.extend(result)
                    pbar.update()

        return duplicate_list


from tqdm.autonotebook import tqdm
from multiprocessing import Pool
from datasketch import MinHash, MinHashLSH
from typing import Optional
from glob import glob
import pickle
import json
from functools import partial 

# due to how datasketch implements their connection with Redis,
# for now we reference the LSH index using this 'singleton'
lsh = None

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
def compute_minhash_jsonl(t: tuple, fname) -> Optional[tuple]:
    lineNo, line = t
    lineNo += 1
    line = json.loads(line)
    line = line.get("text", "")
    s = set(line.split())
    if not s:
        return None
    m = MinHash(num_perm=128)
    for d in s:
        m.update(d.encode('utf8'))
    # generate a unique key for this document
    key = f"{fname}-{lineNo}"
    return (key, m)

"""
Compute minhash signatures for a given jsonl file with the format specified for
'compute_minhash_jsonl' above.

input_file is the path to the singular jsonl file
minhash_dir is the path to store the minhash signatures in
"""
def compute_minhash_signatures_file(infile, minhash_dir):
    n = 50000
    fname = infile.split('/')[-1]
    with open(infile) as fin, Pool(32) as p, tqdm(total=n, desc=fname) as pbar:
        minhash_list = list()
        partial_compute_minhash = partial(compute_minhash_jsonl, fname=fname)
        for result in p.imap_unordered(partial_compute_minhash, enumerate(fin)):
            if result:
                minhash_list.append(result)
                pbar.update()            
        with open(f"{minhash_dir}/{fname[:-6]}.pkl", 'wb') as fp:
            pickle.dump(minhash_list, fp)
        print(f"Generated MinHash for {len(minhash_list):,} documents in {fname}")

"""
Compute minhash signatures for a directory of jsonl files with the format specified for
'compute_minhash_jsonl' above.

input_dir is the path to directory of jsonl files
minhash_dir is the path to store the minhash signatures in
"""
def compute_minhash_signatures(input_dir, minhash_dir):
    for infile in glob(f'{input_dir}/*.jsonl'):
        compute_minhash_signatures_file(infile, minhash_dir)


"""
Deduplicates a MinHash signature corresponding to a document using the provided LSH index.
If the document is not duplicated in the LSH index, it is added to the index.

params - tuple with two elements (key, minhash) where the key uniquely identifies the current document
and minhash is the document's minhash signature

returns a list of tuples of the form (key, dup_key) that identify which documents
from the LSH index have been matched as duplicates with respect to the current document
"""
def deduplicate_and_insert(params):
        # query against lsh index
        key, m_query = params
        result = lsh.query(m_query)
        
        # insert if not duplicated in index
        if not len(result) or (len(result) == 1 and result[0] == key):
            lsh.insert(key, m_query)
        
        return [(key, dup_key) for dup_key in result]

"""
Deduplicate documents in the given minhash file and adds them to the LSH index if appropriate.
Documents without existing duplicates will be stored in the LSH index for future deduplication.

minhashfiles - paths to file of minhash signatures stored in pickle format

returns a list of tuples of the form (key, dup_key) representing duplicated documents,
key is from the corpus we are currently considering and dup_key is from the LSH index.

Note: currently, this should only be run through deduplicate_corpus in order to ensure instantiation of the lsh object
"""
def deduplicate_minhash_file(minhashfile):
    duplicate_list = []
    with open(minhashfile, 'rb') as fin:
            minhash_list = pickle.load(fin)
            fname = minhashfile.split('/')[-1]
            with Pool(32) as p, tqdm(total=len(minhash_list), desc=fname) as pbar:
                for result in p.imap_unordered(deduplicate_and_insert, minhash_list):
                    if result:
                        duplicate_list.extend(result)
                    pbar.update()

    return duplicate_list

"""
Deduplicates documents in the given corpus and adds them to the LSH index if appropriate.
Documents without existing duplicates will be stored in the LSH index for future deduplication.

minhash_files - iterable of paths to files of minhash signatures are stored in pickle format
lsh_params - MinHashLSH object parameters, used to connect to the LSH index

returns a list of tuples of the form (key, dup_key) representing duplicated documents,
key is from the corpus we are currently considering and dup_key is from the LSH index.
"""
def deduplicate_corpus(minhash_files, lsh_params):
    global lsh
    lsh = MinHashLSH(**lsh_params)
    duplicate_list = []
    for minhashfile in minhash_files:
        dups = deduplicate_minhash_file(minhashfile)
        duplicate_list.extend(dups)

    return duplicate_list



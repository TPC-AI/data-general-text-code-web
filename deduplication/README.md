# Deduplication of a Large Corpus with MinHashLSH

Since the corpora are collected from a variety of sources, they may contain duplicates of the same article or articles that share large portions of their text albeit not identical (such as preprints and published articles). Having unknown duplicates in the training corpus will bias the model towards the overrepresented data, consumes unnecessary compute cycles and memory, and produce overfit models that end up memorizing those specific instances rather than learning the underlying patterns. Therefore, deduplication is a necessary step before the corpus can be used in training. However, a brute-force method comparing every pair of documents will take $O(n^2)$ time, so it is not feasible given the size of our corpus.

Here we applied a technique called "minhashing", which compresses large sets in such a way that we can still deduce the similarity of the underlying sets from their compressed versions. By combining minhashing with "locality-sensitive hashing (LSH)", which focuses on pairs that are likely to be similar without investigating every pair, we can achieve sub-linear performance, making deduplicating a corpus as large as ours a possibility. 
(References: Textbook and DataSketch Documentation).


The overall process consists of three major steps: precompute minhash signatures for every document, build an LSH index for the corpus, and query the index for potential duplicates.

## Step 1. Precompute minhash signatures

- Source code: `precompute_minhash_pile.py` and `precompute_minhash_prd.py`

- Output: `/eagle/tpc/hongz/minhash/pile/*.pkl`

This is the most time-consuming part of the process due to heavy I/O (i.e., reading every file in the corpus). To speed up the process, the code is parallelized such that the documents are distributed to all CPU cores to compute their minhash signatures, and the main process will save all the returned signatures in a pickle file. Every minhash signature is saved along with a `key` that is comprised of a file name and a line number so that the signature can be traced back to its original document.

To apply the code to another corpus (other than PILE and PRD), a custom `generator` is required. The generator should accept a file object, and when called, it should return a tuple consisting of a line number and a document from the file, similar to the Python built-in `enumerate()` function.

## Step 2. Build the LSH index (in Redis)

- Source code: `build_lsh_index.py`

- Output: `/eagle/tpc/hongz/minhashlsh_redis_dump/*.rdb`

In this step, we read from the pickle files produced in Step 1 and insert all the minhash signatures to an LSH index, which requires two parameters, `num_perm` and `threshold`. The `num_perm` value should be consistent with Step 1 (default: 128). The `threshold` parameter controls the lower bound of similarity above which a pair of documents would be considered duplicates (default: 0.9). Note that the threshold is decided when building the index and it is not possible to change the threshold at query time without building another index.

Technically, the index can be stored as an in-memory object. However, to enable parallelization and data persistency, Redis is used as the storage layer for the LSH index. The index will take up roughly twice as much memory as the minhash signatures do, so the index for a very large corpus (such as PRD) might exceed the available RAM of a compute node, in which case the signatures need to be partitioned and saved into different indices on different nodes.

Caution: based on experiments, the `lsh.insertion_session()` is NOT thread-safe, so it should not be shared among threads or processes.

## Step 3: Query for duplicates

- Source code: `query_for_duplicates.py`

In the last step, we read minhash signatures from Step 1 and query them against the LSH index built in Step 2. The code is parallelized so that the signatures are distributed to all the CPU cores. The query returns duplicated documents as tuples, in which the first element is always the key of the minhash signature used to query the index, and the second element is the key of a duplicate document found in the index.

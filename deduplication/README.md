# Deduplication of a Large Corpus with MinHashLSH

Since the corpora are collected from a variety of sources, they may contain duplicates of the same article or articles that share large portions of their text albeit not identical (such as preprints and published articles). Having unknown duplicates in the training corpus will bias the model towards the overrepresented data, consumes unnecessary compute cycles and memory, and produce overfit models that end up memorizing those specific instances rather than learning the underlying patterns. Therefore, deduplication is a necessary step before the corpus can be used in training. However, a brute-force method comparing every pair of documents will take $O(n^2)$ time, so it is not feasible given the size of our corpus.

Here we applied a technique called "minhashing", which compresses large sets in such a way that we can still deduce the similarity of the underlying sets from their compressed versions. By combining minhashing with "locality-sensitive hashing (LSH)", which focuses on pairs that are likely to be similar without investigating every pair, we can achieve sub-linear performance, making deduplicating a corpus as large as ours a possibility. 
(See more details: [Textbook](http://infolab.stanford.edu/~ullman/mmds/ch3.pdf) and [DataSketch Documentation](https://ekzhu.com/datasketch/lsh.html)).


The overall process consists of three major steps: precompute minhash signatures for every document, build an LSH index for the corpus, and query the index for potential duplicates.

## Step 1. Precompute minhash signatures

- Source code: `precompute_minhash_pile.py` and `precompute_minhash_arxiv.py`

- Output: `/eagle/tpc/hongz/minhash/pile/*.pkl`

This is the most time-consuming part of the process due to heavy I/O (i.e., reading every file in the corpus). To speed up the process, the code is parallelized such that the documents are distributed to all CPU cores to compute their minhash signatures, and the main process will save all the returned signatures in a pickle file. 

Each minhash signature is stored together with a `key` that provides a unique identifier for a document in the corpus. In the current implementation, the `key` consists of the name of the source file where the document is stored and the line number that indicates where the document starts within that source file.


## Step 2. Build the LSH index (in Redis)

- Source code: `build_lsh_index.py`

- Output: `/eagle/tpc/hongz/minhashlsh_redis_dump/*.rdb`

In this step, we read from the pickle files produced in [Step 1](#step-1-precompute-minhash-signatures) and insert all the minhash signatures to an LSH index, which requires two parameters, `num_perm` and `threshold`. The `num_perm` value should be consistent with [Step 1](#step-1-precompute-minhash-signatures) (default: 128). The `threshold` parameter controls the lower bound of similarity above which a pair of documents would be considered duplicates (default: 0.8). Note that the threshold is decided when building the index and it is not possible to change the threshold at query time without building another index.

Technically, the index can be stored as an in-memory object. However, to enable parallelization and data persistency, Redis is used as the storage layer for the LSH index. The index will take up roughly twice as much memory as the minhash signatures do, so the index for a very large corpus might exceed the available RAM of a compute node, in which case the signatures need to be partitioned and saved into different indices on different nodes.

When Redis is used as the storage backend instead of an in-memory data structure, it is important to set a specific `basename` for the LSH index. This is necessary because multiple indices may be stored in the same Redis database. To query an index stored in Redis, the same `basename` must be used to initialize the LSH object. Otherwise, queries will always result in empty responses. In the current implementation, the `basename` is set to the string `"tpc"`.

According to our experiments, `lsh.insertion_session()` is NOT thread-safe, so it should not be shared among threads or processes.

## Step 3: Query for duplicates

- Source code: `query_for_duplicates.py`

In the last step, we read minhash signatures from [Step 1](#step-1-precompute-minhash-signatures) and query them against the LSH index built in [Step 2](#step-2-build-the-lsh-index-in-redis). It is essential to provide the `basename` when initializing the LSH object if the LSH index is saved in Redis. The `basename` should match the one used in [Step 2](#step-2-build-the-lsh-index-in-redis); otherwise, the queries will yield no results.

The code is parallelized so that the signatures are distributed to all the CPU cores. The query returns duplicated documents as tuples, in which the first element is always the key of the minhash signature used to query the index, and the second element is the key of a duplicate document found in the index.


## Result: Deduplication of ARXIV and PILE

- Output: `duplicates_arxiv_pile_sci.csv`

The deduplication pipeline was tested on the ARXIV dataset (`/eagle/tpc/hongz/arxiv_jsonl/`) and PILE dataset (`/eagle/tpc/Text/jsonl_pile/`). PILE contains documents from a variety of sources, many of which are not scientific articles and are unlikely to find duplicates in the ARXIV dataset. Therefore, only the "ArXiv" and "PubMed Central" subsets of PILE were searched for deduplication with ARXIV.

There are 187,942 files in the ARXIV dataset, and 8,057,644 files in the science subsets of PILE. 93,204 documents in ARXIV were found to have at least 1 duplicate in the ArXiv and PubMed Central subsets of PILE. In total, there are 179,824 pairs of duplicated documents, suggesting that some documents in ARXIV have more than one duplicates in PILE.

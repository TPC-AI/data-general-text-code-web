# Install
```bash
git clone https://github.com/TPC-AI/data-general-text-code-web.git
cd data-general-text-code-web/
pip install .
```

# Usage

```
usage: __main__.py [-h] (--single | --multi | --file) --name NAME [NAME ...] --input INPUT [INPUT ...] --minhash-dir
                   MINHASH_DIR [MINHASH_DIR ...] --output-file OUTPUT_FILE [--sim-threshold SIM_THRESHOLD]
                   [--num-perm NUM_PERM] [--mode {lsh,bloom}] --save-dir SAVE_DIR -n NUM [--fp FP] [--clear]
                   [--redis_port REDIS_PORT] [--skip-minhashing]

CLI Tool for Text Deduplication using MinHashLSH

options:
  -h, --help            show this help message and exit
  --single              Deduplicate a single corpus against the index
  --multi               Deduplicate multiple corpora against the index
  --file                Deduplicate a single JSONL file (may have multiple documents) against the index
  --name NAME [NAME ...]
                        Name(s) of corpus we are deduplicating
  --input INPUT [INPUT ...]
                        <Single or Multi workflow> Directory or directories where jsonl data is stored
                        <File workflow> JSONL file to deduplicate
  --minhash-dir MINHASH_DIR [MINHASH_DIR ...]
                        Output directory where pickled minhash signatures will be stored
  --output-file OUTPUT_FILE
                        Path to csv file where duplicates will be logged
  --sim-threshold SIM_THRESHOLD
                        Jaccard Similarity threshold for deduplication, should be in [0, 1]. Default is 0.8
  --num-perm NUM_PERM   Number of hash functions for MinHashing. Default is 128
  --mode {lsh,bloom}    Whether to use classic MinHashLSH or LSHBloom, default is LSHBloom
  --save-dir SAVE_DIR   <Bloom Mode (Required)> Directory where Bloom Index will be stored
  -n NUM, --num NUM     <Bloom Mode (Required)> Total size of text dataset in number of documents
  --fp FP               <Bloom Mode> False Positive rate for Bloom Filter, should be in [0,1]. Default is 0.01
  --clear               <Bloom Mode> If set, will remove the bloom filter index in save-dir as well as any results csv and start from scratch (Warning: this can not be undone)
  --redis_port REDIS_PORT
                        <LSH mode> The port that Redis server is listening on. Default is 6379
  --skip-minhashing     If set, will skip the minhashing step of each workflow (useful if minhashes have been precomputed at minhash_dir)
```

# Overview
This repo has two deduplication algorithms: vanilla MinHashLSH which uses a redis backend and LSHBloom which uses memory-mapped bloom filters as a backend. By default we use LSHBloom since for large corpora it is far more memory efficient (and for many TB of documents this is significantly more feasible to acquire the necessary resources for). 

If you are using LSHBloom then the index will be stored in whatever `save-dir` you specify when running the CLI. To continue to deduplicate against an existing index, just specify the same `save-dir` on subsequent runs and the tool will load the existing Bloom Filters from disk (ensuring that you deduplicate against whatever documents were already inserted in previous runs). You can use the flag `--clear` to delete the existing index and start from scratch, but this is irreversible and shouldn't be used unless you're intending to rerun every deduplication workflow you've run in the past. Importantly the `--num` parameter should be set to the total expected size of the text dataset you intend to process, so for example if you are currently processing subset A but you know that you will be processing subsets B, C, and D in the future a good value for `--num` is the total number of documents present in all four subsets (or a suitable approximation/upperbound). This parameter is used to set the size of our bloom filters and is ignored if the filters already exist.

For MinHashLSH you'll need to start a redis server, and provide the port number that it is listening on. Similarly to deduplicate against an existing index, just run that redis server and point the tool towards the appropriate port. The only way to clear this index is to delete the redis database itself.

To speed up execution, you may choose to skip the minhashing step IF you have already precomputed the minhash signatures using the `--skip-minhashing` flag. In this scenario the tool will skip attempting to minhash files and will simply read whatever minhash files are present in `minhash-dir`. 

Additionally, you will have to provide an output path for a csv file where the tool will append the duplicates for the corpora you are currently processing. 

# Recipes

Add `--skip-minhashing` and `--clear` as needed.

## Deduplicate a single corpus internally

```shell
python -m deduplication --single --name acm_test --input ~/data/acm_test/ --minhash-dir ./project/minhash/ACM_test/ --save-dir ./project/testsingle/ --output-file ./project/testsingle/result.csv --num 1721 
```

## Deduplicate multiple corpora against one another

```shell
# sizeof(A+B) = 1600000

python -m deduplication --multi --name acm_test rp1_arxiv --input ~/data/acm_test/ ~/data/RP1/arxiv/ --minhash-dir ./project/minhash/ACM_test/  ./project/minhash/RP1_arxiv/ --save-dir ./project/testmulti/ --output-file ./project/testmulti/result.csv --num 1600000 
```

## Deduplicate a single JSONL file (of potentially many documents)
```shell
python -m deduplication --file --name pes2o --input ~/data/peS2o/JSON_data/train-00000-of-00020.json --minhash-dir ./project/minhash/peS2o/ --save-dir ./project/testmulti/ --output-file ./project/testmulti/result.csv --num 1600000
```
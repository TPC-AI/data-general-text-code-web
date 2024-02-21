from workflows import dedup_internal

input_dir = "/eagle/argonne_tpc/TextCollections/RP1/arxiv"
minhash_dir = "/eagle/argonne_tpc/arham/minhash/RP1_arxiv_test"
csvfile = "./rp1_self.csv"
port = 6682

dedup_internal(input_dir, minhash_dir, csvfile, sim_threshold=0.8, n_hash_funcs=128, redis_name=b'tpc', redis_port=port, compute_minhashes=False)
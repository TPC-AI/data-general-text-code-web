from datasketch import MinHashLSH
from ingest import compute_minhash_signatures, deduplicate_corpus
from writers import write_duplicates_to_csv

# workflow for deduping single corpus internally
def dedup_internal(input_dir, minhash_dir, csvfile, sim_threshold=0.8, n_hash_funcs=128, redis_name=b'tpc', redis_port=6379, compute_minhashes=True):
	lsh_params = {
		'threshold': sim_threshold, 
		'num_perm': n_hash_funcs,
		'storage_config': {
			'type': 'redis',
			'basename': redis_name,
			'redis': {'host': 'localhost', 'port': redis_port},
		}
	}

	if compute_minhashes:
		compute_minhash_signatures(input_dir, minhash_dir)

	duplicates = deduplicate_corpus(minhash_dir, lsh_params)
	write_duplicates_to_csv(duplicates, csvfile)


# workflow for deduping a single corpus against a bunch of minhash dirs

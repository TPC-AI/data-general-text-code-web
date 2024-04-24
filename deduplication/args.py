import sys
import argparse

# cmd arguments
def parse_args():
	cmd_args = " ".join(sys.argv)
	parser = argparse.ArgumentParser(
		description="CLI Tool for Text Deduplication using MinHashLSH"
	)
	group = parser.add_mutually_exclusive_group(required=True)
	group.add_argument(
		"--single",
		action="store_true",
		help="Deduplicate a single corpus against the index",
	)
	group.add_argument(
		"--multi",
		action="store_true",
		help="Deduplicate multiple corpora against the index",
	)
	group.add_argument(
		"--file",
		action="store_true",
		help="Deduplicate a single JSONL file (may have multiple documents) against the index",
	)
	parser.add_argument(
		"--name",
		help="Name(s) of corpus we are deduplicating",
		required=True,
		nargs="+",
	)
	parser.add_argument(
		"--input-dir",
		help="Directory or directories where jsonl data is stored",
		required=True,
		nargs="+",
	)
	parser.add_argument(
		"--minhash-dir",
		help="Output directory where pickled minhash signatures will be stored",
		required=True,
		nargs="+",
	)
	parser.add_argument(
		"--sim_threshold",
		help="Jaccard Similarity threshold for deduplication, should be in [0, 1]",
		default=0.8,
	)
	parser.add_argument(
		"--num-perm",
		help="Number of hash functions for MinHashing",
		default=128,
	)
	parser.add_argument(
		"--mode",
		default="bloom",
		choices=["lsh", "bloom"],
		help="Whether to use classic MinHashLSH or LSHBloom",
	)
	parser.add_argument(
		"--save-dir",
		help="<Bloom Mode (Required)> Directory where Bloom Index will be stored",
		required=("--mode lsh" not in cmd_args)
	)
	parser.add_argument(
		"-n",
		"--num",
		type=int,
		help="<Bloom Mode (Required)> Total size of text dataset in number of documents",
		required=("--mode lsh" not in cmd_args),
	)
	parser.add_argument(
		"--fp",
		type=float,
		help="<Bloom Mode> False Positive rate for Bloom Filter",
		default=0.01,
	)
	parser.add_argument(
		"--redis_port",
		help="<LSH mode> The port that Redis server is listening on. Default is 6379.",
		type=int,
		default=6379,
	)

	return parser.parse_args()

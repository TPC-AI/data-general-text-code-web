import sys
import argparse

# cmd arguments
def parse_args():
	cmd_args = " ".join(sys.argv)
	parser = argparse.ArgumentParser(
		description="CLI Tool for Text Deduplication using MinHashLSH",
		formatter_class=argparse.RawTextHelpFormatter
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
		"--input",
		help="<Single or Multi workflow> Directory or directories where jsonl data is stored\n<File workflow> JSONL file to deduplicate",
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
		"--output-file",
		help="Path to csv file where duplicates will be logged",
		required=True,
	)
	parser.add_argument(
		"--sim-threshold",
		help="Jaccard Similarity threshold for deduplication, should be in [0, 1]. Default is 0.8",
		default=0.8,
	)
	parser.add_argument(
		"--num-perm",
		help="Number of hash functions for MinHashing. Default is 128",
		default=128,
	)
	parser.add_argument(
		"--mode",
		default="bloom",
		choices=["lsh", "bloom"],
		help="Whether to use classic MinHashLSH or LSHBloom, default is LSHBloom",
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
		help="<Bloom Mode> False Positive rate for Bloom Filter, should be in [0,1]. Default is 0.01",
		default=0.01,
	)
	parser.add_argument(
		"--redis_port",
		help="<LSH mode> The port that Redis server is listening on. Default is 6379",
		type=int,
		default=6379,
	)
	parser.add_argument(
		"--skip-minhashing",
		help="If set, will skip the minhashing step of each workflow (useful if minhashes have been precomputed at minhash_dir)",
		action="store_false"
	)

	return parser.parse_args()

from workflows import *
from args import parse_args

args = parse_args()

if args.mode == "bloom":
	if args.single:
		assert len(args.input) == 1 and len(args.minhash_dir) == 1 and len(args.name) == 1, "Expected single input argument but got a list" 
		dedup_single_bloom(args.input[0], args.minhash_dir[0], args.num, args.fp, args.output_file, args.name[0], args.sim_threshold, args.num_perm, args.save_dir, not args.skip_minhashing)
	elif args.multi:
		dedup_multi_bloom(args.input, args.minhash_dir, args.num, args.fp, args.output_file, args.name, args.sim_threshold, args.num_perm, args.save_dir, not args.skip_minhashing)
	else:
		assert len(args.input) == 1 and len(args.minhash_dir) == 1 and len(args.name) == 1, "Expected single input argument but got a list" 
		dedup_single_file_bloom(args.input[0], args.minhash_dir[0], args.num, args.fp, args.output_file, args.name[0], args.sim_threshold, args.num_perm, args.save_dir, not args.skip_minhashing)
else:
	if args.single:
		assert len(args.input) == 1 and len(args.minhash_dir) == 1 and len(args.name) == 1, "Expected single input argument but got a list" 
		dedup_single_lsh(args.input[0], args.minhash_dir[0], args.num, args.fp, args.output_file, args.name[0], args.sim_threshold, args.num_perm, args.save_dir, not args.skip_minhashing)
	elif args.multi:
		dedup_multi_lsh(args.input, args.minhash_dir, args.num, args.fp, args.output_file, args.name, args.sim_threshold, args.num_perm, args.save_dir, not args.skip_minhashing)
	else:
		assert len(args.input) == 1 and len(args.minhash_dir) == 1 and len(args.name) == 1, "Expected single input argument but got a list" 
		dedup_single_file_lsh(args.input[0], args.minhash_dir[0], args.num, args.fp, args.output_file, args.name[0], args.sim_threshold, args.num_perm, args.save_dir, not args.skip_minhashing)



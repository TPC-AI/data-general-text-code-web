from deduplication.args import parse_args
from deduplication.parsl_conf import get_compute_settings
from deduplication.workflows import (
    dedup_multi_bloom,
    dedup_multi_lsh,
    dedup_single_bloom,
    dedup_single_file_bloom,
    dedup_single_file_lsh,
    dedup_single_lsh,
)

if __name__ == "__main__":
    args = parse_args()
    # Setup parsl if applicable
    parsl_config = None
    if args.parsl_config is not None:
        parsl_config = get_compute_settings(args)

    if args.mode == "bloom":
        if args.single:
            assert (
                len(args.input) == 1
                and len(args.minhash_dir) == 1
                and len(args.name) == 1
            ), "Expected single input argument but got a list"
            dedup_single_bloom(
                args.input[0],
                args.minhash_dir[0],
                args.num,
                args.fp,
                args.output_file,
                args.name[0],
                args.sim_threshold,
                args.num_perm,
                args.save_dir,
                not args.skip_minhashing,
                args.clear,
                parsl_config,
            )
        elif args.multi:
            dedup_multi_bloom(
                args.input,
                args.minhash_dir,
                args.num,
                args.fp,
                args.output_file,
                args.name,
                args.sim_threshold,
                args.num_perm,
                args.save_dir,
                not args.skip_minhashing,
                args.clear,
                parsl_config,
            )
        else:
            assert (
                len(args.input) == 1
                and len(args.minhash_dir) == 1
                and len(args.name) == 1
            ), "Expected single input argument but got a list"
            dedup_single_file_bloom(
                args.input[0],
                args.minhash_dir[0],
                args.num,
                args.fp,
                args.output_file,
                args.name[0],
                args.sim_threshold,
                args.num_perm,
                args.save_dir,
                not args.skip_minhashing,
                args.clear,
                parsl_config,
            )
    else:
        if args.single:
            assert (
                len(args.input) == 1
                and len(args.minhash_dir) == 1
                and len(args.name) == 1
            ), "Expected single input argument but got a list"
            dedup_single_lsh(
                args.input[0],
                args.minhash_dir[0],
                args.output_file,
                args.name[0],
                args.sim_threshold,
                args.num_perm,
                redis_port=args.redis_port,
                compute_minhashes=not args.skip_minhashing,
            )
        elif args.multi:
            dedup_multi_lsh(
                args.input,
                args.minhash_dir,
                args.output_file,
                args.name,
                args.sim_threshold,
                args.num_perm,
                redis_port=args.redis_port,
                compute_minhashes=not args.skip_minhashing,
            )
        else:
            assert (
                len(args.input) == 1
                and len(args.minhash_dir) == 1
                and len(args.name) == 1
            ), "Expected single input argument but got a list"
            dedup_single_file_lsh(
                args.input[0],
                args.minhash_dir[0],
                args.output_file,
                args.name[0],
                args.sim_threshold,
                args.num_perm,
                redis_port=args.redis_port,
                compute_minhashes=not args.skip_minhashing,
            )

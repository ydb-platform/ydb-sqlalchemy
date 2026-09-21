import logging

from options import parse_options
from workload import run_from_args

if __name__ == "__main__":
    args = parse_options()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(threadName)s %(message)s",
    )
    run_from_args(args)

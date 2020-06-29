import os
import argparse
import logging
import sys
from fuse import FUSE
from .httpfs import HttpFs
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="""usage: simple-s3fs <mountpoint>""")

    parser.add_argument('mountpoint')

    parser.add_argument(
        '-f', '--foreground',
        action='store_true',
        default=False,
        help='Run in the foreground')

    parser.add_argument(
        '--block-size',
        default=2**20,type=int
    )

    parser.add_argument(
        '--disk-cache-size',
        default=2**30,
        type=int)

    parser.add_argument(
        '--disk-cache-dir',
        default='/tmp/xx')

    parser.add_argument(
        '--lru-capacity',
        default=400,
        type=int)

    parser.add_argument(
        '--aws-profile',
        default=None,
        type=str)

    parser.add_argument(
        '-l', '--log',
        default=None,
        type=str)

    args = vars(parser.parse_args())

    if not os.path.isdir(args['mountpoint']):
        try:
            Path(args['mountpoint']).mkdir(mode=0o644, parents=True, exist_ok=True)
        except OSError as e:
            print("Mount point must be a directory: {}".format(args['mountpoint']),
                file=sys.stderr)
            print(e.strerror, file=sys.stderr)
            #sys.exit(1)

    logger = logging.getLogger('simple-s3fs')
    logger.setLevel(logging.INFO)

    if args['log']:
        hdlr = logging.FileHandler(args['log'])
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(module)s: %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)

    bucket = args['mountpoint'].split('/')[-1]


    start_msg = """
Mounting HTTP Filesystem...
    bucket: {bucket}
    mountpoint: {mountpoint}
    foreground: {foreground}
""".format(bucket=bucket,
           mountpoint=args['mountpoint'],
           foreground=args['foreground'])
    print(start_msg, file=sys.stderr)

    fuse = FUSE(
        HttpFs(
               bucket,
               disk_cache_size=args['disk_cache_size'],
               disk_cache_dir=args['disk_cache_dir'],
               lru_capacity=args['lru_capacity'],
               block_size=args['block_size'],
               aws_profile=args['aws_profile'],
               logger = logger
            ),
        args['mountpoint'],
        foreground=args['foreground'],
        attr_timeout=0.0,
        entry_timeout=0.0
    )


if __name__ == "__main__":
    main()

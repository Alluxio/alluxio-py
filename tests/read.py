#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Read a file from Alluxio and write it to a local file.

This script starts a single Python process for reading a file from Alluxio,
and streaming it to a local file.
"""

from __future__ import print_function
import argparse
import time

import syspath
import alluxio


def read(host, port, src, dst):
    """Read the {src} file from Alluxio and write it to the {dst} file in the local filesystem.

    Args:
        host: The Alluxio proxy's hostname.
        port: The Alluxio proxy's web port.
        src: The file in Alluxio to be read from.
        dst: The file in the local filesystem to be written to.

    Returns:
        The total time (seconds) used to read the file from Alluxio and write it to the local filesystem.
    """


    start = time.time()
    c = alluxio.Client(host, port)
    with c.open(src, 'r', recursive=True) as alluxio_file:
        with open(dst, 'w') as local_file:
            while True:
                chunk_size = 4 * 1024  # 4KB
                data = alluxio_file.read(chunk_size)
                if data == '':
                    break
                local_file.write(data)
    return time.time() - start


def main(args):
    total_time = read(args.host, args.port, args.src, args.dst)
    print('%d seconds' % total_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Read a file from Alluxio and write it to a local file')
    parser.add_argument('--host', default='localhost',
                        help='Alluxio proxy server hostname')
    parser.add_argument('--port', type=int, default=39999,
                        help='Alluxio proxy server web port')
    parser.add_argument('--src', required=True,
                        help='path to the Alluxio file source')
    parser.add_argument('--dst', required=True,
                        help='path to the local file destination')
    args = parser.parse_args()

    main(args)

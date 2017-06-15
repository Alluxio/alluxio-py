# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Write a local file into Alluxio.

This script starts a single Python process for writing a file in the local
filesystem to Alluxio. The total time of reading the local file stream, and
writing the file stream to Alluxio, is reported at the end of the script.
"""

import argparse
import time

import syspath
import alluxio


def write(host, port, src, dst):
    c = alluxio.Client(host, port)
    with c.open(dst, 'w', recursive=True) as alluxio_file:
        with open(src, 'r') as local_file:
            alluxio_file.write(local_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Write a local file to Alluxio')
    parser.add_argument('--host', default='localhost',
                        help='Alluxio proxy server hostname')
    parser.add_argument('--port', type=int, default=39999,
                        help='Alluxio proxy server web port')
    parser.add_argument('--src', default='data/5mb.txt',
                        help='path to the local file source')
    parser.add_argument('--dst', required=True,
                        help='path to the Alluxio file destination')
    args = parser.parse_args()

    write(args.host, args.port, args.src, args.dst)

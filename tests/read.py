# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Read a file from Alluxio and write it to a local file.

This script starts a single Python process for reading a file from Alluxio,
and write the file stream to a local file.
The total time of reading the Alluxio file stream, and writing the file stream
to local filesystem, is reported at the end of the script.
"""


import sys
sys.path.append('..')  # so that alluxio module can be found

import argparse
import time

import alluxio


def read(host, port, src, dst):
    c = alluxio.Client(host, port)
    start_time = time.time()
    with c.open(src, 'r', recursive=True) as alluxio_file:
        with open(dst, 'w') as local_file:
            while True:
                chunk_size = 4 * 1024  # 4KB
                data = alluxio_file.read(chunk_size)
                if data == '':
                    break
                local_file.write(data)
    elapsed_time = time.time() - start_time
    print '%d seconds' % elapsed_time


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

    read(args.host, args.port, args.src, args.dst)

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script starts one python process to write to Alluxio.

It should be run from its parent directory.
"""

from __future__ import print_function
import argparse
import os
import time

import syspath
import alluxio
from utils import alluxio_path


def print_stats(args, total_time):
    src_bytes = os.stat(args.src).st_size
    total_file_size = src_bytes * args.iteration
    throughput = total_file_size / total_time

    print('Number of iterations: %d' % args.iteration)
    print("Total file size: %d bytes" % total_file_size)
    print('Total time: %f seconds' % total_time)
    print('Write throughput : %f bytes/second' % throughput)


def main(args):
    with open(args.src, 'r') as f:
        data = f.read()
    total_time = 0
    c = alluxio.Client(args.host, args.port)
    for iteration in range(args.iteration):
        dst = alluxio_path(args.dst, iteration, 0, 0):
        write_type = alluxio.wire.WriteType(args.write_type)
        print('Iteration %d ... ' % iteration, end='')
        start_time = time.time()
        with c.open(dst, 'w', recursive=True, write_type=write_type) as f:
            f.write(data)
        elapsed_time = time.time() - start_time
        print('{} seconds'.format(elapsed_time))
        total_time += elapsed_time
    print_stats(args, total_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Start one python process to write to Alluxio')
    parser.add_argument('--host', default='localhost',
                        help='Alluxio proxy server hostname')
    parser.add_argument('--port', type=int, default=39999,
                        help='Alluxio proxy server web port')
    parser.add_argument('--src', default='data/5mb.txt',
                        help='path to the local file source')
    parser.add_argument('--dst', default='/alluxio-py-test',
                        help='path to the root directory for all written file')
    parser.add_argument('--iteration', type=int, default=1,
                        help='number of iterations to repeat the concurrent writing')
    parser.add_argument('--write-type', default='MUST_CACHE',
                        help='write type for creating the file, see alluxio.wire.WriteType')
    args = parser.parse_args()
    main(args)

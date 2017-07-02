#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script starts one python process to read from Alluxio.

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
    src_bytes = os.stat(args.expected).st_size
    total_file_size = src_bytes * args.iteration
    throughput = total_file_size / total_time

    print('Number of iterations: %d' % args.iteration)
    print("Total file size: %d bytes" % total_file_size)
    print('Total time: %f seconds' % total_time)
    print('Read throughput: %f bytes/second' % throughput)


def main(args):
    with open(args.expected, 'r') as f:
        expected = f.read()
    total_time = 0
    c = alluxio.Client(args.host, args.port)
    for iteration in range(args.iteration):
        src = alluxio_path(args.src, iteration, 0, 0)
        print('Iteration %d ... ' % iteration, end='')
        start_time = time.time()
        with c.open(src, 'r') as f:
            f.read()
        elapsed_time = time.time() - start_time
        print('{} seconds'.format(elapsed_time))
        total_time += elapsed_time
    print_stats(args, total_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Start one python process to read from Alluxio')
    parser.add_argument('--host', default='localhost',
                        help='Alluxio proxy server hostname')
    parser.add_argument('--port', type=int, default=39999,
                        help='Alluxio proxy server web port')
    parser.add_argument('--src', default='/alluxio-py-test',
                        help='path to the Alluxio directory for files to be read from')
    parser.add_argument('--expected', default='data/5mb.txt',
                        help='the path to a file in local filesystem whose content is expected to be the same as those files read from Alluxio')
    parser.add_argument('--iteration', type=int, default=1,
                        help='number of iterations to repeat the concurrent reading')
    args = parser.parse_args()
    main(args)

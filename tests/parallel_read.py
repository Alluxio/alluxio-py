#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script starts {nprocess} python processes in parallel, each process reads
a file stream from Alluxio and compares it against a local file.

This script should be run from its parent directory.
"""

from __future__ import print_function
import argparse
from multiprocessing import Process, Queue
import os
import time

import syspath
import alluxio
from utils import *


def read(host, port, src, expected, queue):
    """Read the {src} file from Alluxio and compare it to the {expected} file in the local filesystem.

    If the Alluxio file is different from the expected file, an AssertionError will be raised.

    Args:
        host: The Alluxio proxy's hostname.
        port: The Alluxio proxy's web port.
        src: The file in Alluxio to be read from.
        expected: The expected content of the file read from Alluxio.
        queue: The queue shared by multiple processes.
    """

    c = alluxio.Client(host, port)
    start = time.time()
    with c.open(src, 'r') as alluxio_file:
        data = alluxio_file.read()
    alluxio_read_time = time.time() - start
    assert data == expected
    queue.put(alluxio_read_time)


def run_read(args, expected, iteration_id, process_id, queue):
    src = alluxio_path(args.src, iteration_id, args.node, process_id) if args.node else args.src
    read(args.host, args.port, src, expected, queue)


def print_stats(args, total_time):
    client = alluxio.Client(args.host, args.port)
    # assume all files have the same size.
    alluxio_file = alluxio_path(args.src, 0, args.node, 0) if args.node else args.src
    src_bytes = client.get_status(alluxio_file).length
    average_time = total_time / args.iteration
    average_throughput = src_bytes / average_time

    print('Number of iterations: %d' % args.iteration)
    print('Number of processes per iteration: %d' % args.nprocess)
    print('File size: %d bytes' % src_bytes)
    print('Total time: %f seconds' % total_time)
    print('Average time for each iteration: %f seconds' % average_time)
    print('Average read throughput: %f bytes/second' % average_throughput)


def main(args):
    with open(args.expected, 'r') as f:
        expected = f.read()

    queue = Queue()
    for iteration in range(args.iteration):
        print('Iteration %d ... ' % iteration, end='')
        processes = []
        for process_id in range(args.nprocess):
            p = Process(target=run_read, args=(args, expected, iteration, process_id, queue))
            p.start()
            processes.append(p)
        for p in processes:
            p.join()
        print('done')
    total_time = 0
    for _ in processes:
        total_time += queue.get()
    print_stats(args, total_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Start multiple python processes to read a Alluxio file in parallel')
    parser.add_argument('--nprocess', type=int, default=1,
                        help='number of python processes, each process runs read.py')
    parser.add_argument('--host', default='localhost',
                        help='Alluxio proxy server hostname')
    parser.add_argument('--port', type=int, default=39999,
                        help='Alluxio proxy server web port')
    parser.add_argument('--src', required=True,
                        help='path to the Alluxio file to be read from or the Alluxio directory \
                        containing all data written by parallel_write.py, \
                        if this a file, --node must not be set')
    parser.add_argument('--expected', default='data/5mb.txt',
                        help='the path to a file in local filesystem whose content is expected to be the same as those files read from Alluxio')
    parser.add_argument(
        '--node', help='a unique identifier to another node to read data from, if this not set, --src must be a path to an Alluxio file')
    parser.add_argument('--iteration', type=int, default=1,
                        help='number of iterations to repeat the concurrent reading')
    args = parser.parse_args()

    mkdir_p('logs')

    main(args)

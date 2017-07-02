#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script starts {nprocess} python processes in parallel, each process reads
a file stream from Alluxio and compares it against a local file.

This script should be run from its parent directory.
"""

from __future__ import print_function
import argparse
from multiprocessing import Process, Value
import os
import sys
import time

import syspath
import alluxio
from utils import *


def read(client, src, expected, timer):
    """Read the {src} file from Alluxio and compare it to the {expected} file in the local filesystem.

    If the Alluxio file is different from the expected file, an AssertionError will be raised.

    Args:
        client: Alluxio client.
        src: The file in Alluxio to be read from.
        expected: The expected content of the file read from Alluxio.
        timer: Timer for summing up the total time for reading the files.

    Returns:
        int: Reading time.
    """

    start = time.time()
    with client.open(src, 'r') as alluxio_file:
        data = alluxio_file.read()
    alluxio_read_time = time.time() - start
    assert data == expected
    with timer.get_lock():
        timer.value += alluxio_read_time
    return alluxio_read_time


def run_read(args, expected, iteration_id, process_id, timer):
    client = alluxio.Client(args.host, args.port)
    for iteration in range(args.iteration):
        print('process {}, iteration {} ... '.format(process_id, iteration), end='')
        src = alluxio_path(args.src, iteration_id, args.node, process_id) if args.node else args.src
        t = read(client, src, expected, timer)
        print('{} seconds'.format(t))
        sys.stdout.flush() # https://stackoverflow.com/questions/2774585/child-processes-created-with-python-multiprocessing-module-wont-print


def print_stats(args, average_time_per_process):
    client = alluxio.Client(args.host, args.port)
    # assume all files have the same size.
    alluxio_file = alluxio_path(args.src, 0, args.node, 0) if args.node else args.src
    src_bytes = client.get_status(alluxio_file).length
    average_time_per_iteration_per_process = average_time_per_process / args.iteration
    average_throughput_per_client = src_bytes / average_time_per_iteration_per_process
    average_throughput_per_node = src_bytes * args.nprocess * args.iteration / average_time_per_process

    print('Number of iterations: %d' % args.iteration)
    print('Number of processes per iteration: %d' % args.nprocess)
    print('File size: %d bytes' % src_bytes)
    print('Average time for each process: %f seconds' % average_time_per_process)
    print('Average time for each iteration of a process: %f seconds' % average_time_per_iteration_per_process)
    print('Average read throughput of each client: %f bytes/second' % average_throughput_per_client)
    print('Average read throughput per node: %f bytes/second' % average_throughput_per_node)


def main(args):
    with open(args.expected, 'r') as f:
        expected = f.read()

    timer = Value('d', 0)
    processes = []
    for process_id in range(args.nprocess):
        p = Process(target=run_read, args=(args, expected, iteration, process_id, timer))
        processes.append(p)
    start_time = time.time()
    for p in processes:
        p.start()
    for p in processes:
        p.join()
    average_time_per_process = timer.value / len(processes)
    print_stats(args, average_time_per_process)


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
    main(args)

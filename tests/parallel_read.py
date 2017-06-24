#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script starts {nprocess} python processes in parallel, each process runs
read.py to read a file stream from Alluxio and write it to a local file unique for
each process.

By default, each python process has an ID, starting from 0.
For each process, the Alluxio file is written to local filesystem
{dst}/iteration_{iteration_id}/process_{ID}.

This script should be run from its parent directory.
"""

from __future__ import print_function
import argparse
from multiprocessing import Process
import os
import time

import syspath
import alluxio
from utils import *


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

    mkdir_p(os.path.dirname(dst))
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


def run_read(args, iteration_id, process_id):
    src = alluxio_path(args.src, iteration_id, args.node, process_id) if args.node else args.src
    dst = local_path(args.dst, iteration_id, process_id)
    read(args.host, args.port, src, dst)


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
    total_time = 0
    os.mkdir(args.dst)
    for iteration in range(args.iteration):
        print('Iteration %d ... ' % iteration, end='')
        start_time = time.time()
        processes = []
        for process_id in range(args.nprocess):
            p = Process(target=run_read, args=(args, iteration, process_id))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()
        elapsed_time = time.time() - start_time
        print('%d seconds' % elapsed_time)
        total_time += elapsed_time
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
    parser.add_argument('--dst', required=True,
                        help='the local filesystem directory to store the files read from Alluxio')
    parser.add_argument(
        '--node', help='a unique identifier to another node to read data from, if this not set, --src must be a path to an Alluxio file')
    parser.add_argument('--iteration', type=int, default=1,
                        help='number of iterations to repeat the concurrent reading')
    args = parser.parse_args()

    mkdir_p('logs')

    main(args)

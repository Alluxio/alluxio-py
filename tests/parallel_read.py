#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script starts {nprocess} python processes in parallel, each process runs
read.py to read a file stream from Alluxio and write it to a local file unique for
each process.

By default, each python process has an ID, starting from 0.
For each process, the Alluxio file is written to local filesystem {dst}/{ID}.txt.
The log of each python process is logs/{start time of this script}-{ID}.txt.

This script should be run from its parent directory.
"""

from __future__ import print_function
import argparse
from multiprocessing import Process
import os
import shutil
import sys
import time

import syspath
import alluxio
from read import read


LOG_PREFIX = '-'.join(time.ctime().split(' '))

def log(process_id):
    global script_start_time
    return 'logs/%s-%d.txt' % (LOG_PREFIX, process_id)


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


def run_read(args, process_id):
    dst = '%s/%d.txt' % (args.dst, process_id)
    read(args.host, args.port, args.src, dst)


def print_stats(args, total_time):
    client = alluxio.Client(args.host, args.port)
    src_bytes = client.get_status(args.src).length
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
    for iteration in xrange(args.iteration):
        print('Iteration %d ... ' % iteration, end='')
        os.mkdir(args.dst)

        start_time = time.time()
        processes = []
        for process_id in xrange(args.nprocess):
            p = Process(target=run_read, args=(args, process_id))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()
        elapsted_time = time.time() - start_time
        print('%d seconds' % elapsted_time)
        total_time += elapsted_time

        if iteration < args.iteration - 1:
            shutil.rmtree(args.dst)

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
                        help='path to the Alluxio file source')
    parser.add_argument('--dst', required=True,
                        help='the local filesystem directory to store the files read from Alluxio')
    parser.add_argument('--iteration', type=int, default=1,
                        help='number of iterations to repeat the concurrent reading')
    args = parser.parse_args()

    try:
        os.mkdir('logs')
    except OSError:
        # logs already exists.
        pass

    main(args)
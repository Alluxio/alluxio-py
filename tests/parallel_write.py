#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script starts {nprocess} python processes in parallel, each process writes a local file stream into Alluxio.

By default, each python process has an ID consecutively starting from 0 to
{nprocess}.
For each process, the local file data/5mb.txt is written to Alluxio file
/{dst}/iteration_{iteration_id}/node_{node_id}/process_{ID}.

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
from utils import alluxio_path


def write(client, data, dst, write_type, timer):
    """Write the {src} file in the local filesystem to the {dst} file in Alluxio.

    Args:
        client (:class:`alluxio.Client`): Alluxio client.
        data (str): The file content of the source.
        dst (str): The file to be written to Alluxio.
        write_type (:class:`alluxio.wire.WriteType`): Write type for creating the file.
        timer (:class:`multiprocessing.Value`): Timer for summing up the total time for writing the files.

    Returns:
        float: writing time
    """

    start_time = time.time()
    with client.open(dst, 'w', recursive=True, write_type=write_type) as alluxio_file:
        alluxio_file.write(data)
    elapsed_time = time.time() - start_time
    with timer.get_lock():
        timer.value += elapsed_time
    return elapsed_time


def run_write(args, data, process_id, timer):
    client = alluxio.Client(args.host, args.port)
    for iteration in range(args.iteration):
        print('process {}, iteration {} ... '.format(process_id, iteration), end='')
        dst = alluxio_path(args.dst, iteration, args.node, process_id)
        write_type = alluxio.wire.WriteType(args.write_type)
        t = write(client, data, dst, write_type, timer)
        print('{} seconds'.format(t))
        sys.stdout.flush() # https://stackoverflow.com/questions/2774585/child-processes-created-with-python-multiprocessing-module-wont-print


def print_stats(args, average_time_per_process):
    src_bytes = os.stat(args.src).st_size
    average_time_per_iteration_per_process = average_time_per_process / args.iteration
    average_throughput_per_client = src_bytes / average_time_per_iteration_per_process
    average_throughput_per_node = src_bytes * args.nprocess * args.iteration / average_time_per_process

    print('Number of iterations: %d' % args.iteration)
    print('Number of processes per iteration: %d' % args.nprocess)
    print("File size: %d bytes" % src_bytes)
    print('Average time for each process: %f seconds' % average_time_per_process)
    print('Average time for each iteration of each process: %f seconds' % average_time_per_iteration_per_process)
    print('Average write throughput per python client: %f bytes/second' % average_throughput_per_client)
    print('Average write throughput per node: %f bytes/second' % average_throughput_per_node)


def main(args):
    with open(args.src, 'r') as f:
        data = f.read()

    timer = Value('d', 0)
    processes = []
    for process_id in range(args.nprocess):
        p = Process(target=run_write, args=(args, data, process_id, timer))
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
        description='Start multiple python processes to write local file to Alluxio in parallel')
    parser.add_argument('--nprocess', type=int, default=1,
                        help='number of python processes, each process runs write.py')
    parser.add_argument('--host', default='localhost',
                        help='Alluxio proxy server hostname')
    parser.add_argument('--port', type=int, default=39999,
                        help='Alluxio proxy server web port')
    parser.add_argument('--src', default='data/5mb.txt',
                        help='path to the local file source')
    parser.add_argument('--dst', default='/alluxio-py-test',
                        help='path to the root directory for all written file')
    parser.add_argument('--node', required=True,
                        help='a unique identifier of this node')
    parser.add_argument('--iteration', type=int, default=1,
                        help='number of iterations to repeat the concurrent writing')
    parser.add_argument('--write-type', default='MUST_CACHE',
                        help='write type for creating the file, see alluxio.wire.WriteType')
    args = parser.parse_args()
    main(args)

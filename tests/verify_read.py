#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script verifies that all files under the {dst} directory are the same as
the {src} file in Alluxio.
It uses the Alluxio shell to cat the {src} file to a temporary local file, and
uses diff to compare it with the local files under the {dst} directory.
The file names under {dst} are expected to be of format
{dst}/iteration_{iteration_id}/process_{process_id}.
Multiple files will be verified in parallel depending on the number of the
machine's CPU cores.
"""

from __future__ import print_function
import argparse
from multiprocessing import Pool
import os
import subprocess
import tempfile

from utils import *


def verify_file(iteration_process_pair):
    global args
    iteration_id, process_id = iteration_process_pair
    # Cat the Alluxio source file to a temporary file.
    tmp = tempfile.mkstemp()[1]
    alluxio = os.path.join(args.home, 'bin', 'alluxio')
    if args.node:
        alluxio_file = alluxio_path(args.src, iteration_id, args.node, process_id)
    else:
        alluxio_file = args.src
    cat_cmd = '%s fs cat %s > %s' % (alluxio, alluxio_file, tmp)
    subprocess.check_call(cat_cmd, shell=True)

    # Diff between the file read from Alluxio and the source file in the local
    # filesystem.
    local_file = local_path(args.dst, iteration_id, process_id)
    print('comparing Alluxio file %s to local file %s ... ' %
          (alluxio_file, local_file))
    diff_cmd = 'diff %s %s' % (tmp, local_file)
    subprocess.check_output(diff_cmd, shell=True)
    print('verified that %s was read correctly' % alluxio_file)


def main():
    global args
    iteration_process_pairs = [(iteration, process) for iteration in range(args.iteration)
                               for process in range(args.nprocess)]

    pool = Pool(args.process_pool_size)
    pool.map(verify_file, iteration_process_pairs)
    print('Success!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Verify that files are read correctly')
    parser.add_argument('--home', required=True,
                        help='path to Alluxio home directory')
    parser.add_argument('--src', required=True,
                        help='path to the Alluxio file source or the root Alluxio \
                        directory for all files written by parallel_write.py, \
                        if this is a file, --node must not be set')
    parser.add_argument('--dst', required=True, help='path to the directory for \
        all the data written to the local filesystem')
    parser.add_argument('--node', help='a unique identifier of this node, if this is not set, \
                        --src must be a path to an Alluxio file')
    parser.add_argument('--iteration', type=int, required=True,
                        help='number of iterations of the read task to be verified')
    parser.add_argument('--nprocess', type=int, required=True,
                        help='number of processes of the read task to be verified')
    parser.add_argument('--process-pool-size', type=int, default=4,
                        help='number of concurrent processes for verifying files')
    args = parser.parse_args()

    main()

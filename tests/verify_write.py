#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script verifies that all files under the {dst} directory in Alluxio are
the same as the {src} file in the local filesystem.
It uses the Alluxio shell to cat each file to a temporary local file, and uses
diff to compare it with the local file {src}.
The file names under {dst} are expected to be of format
{dst}/iteration_{iteration_id}/node_{node_id}/process_{process_id}.
Multiple files may be verified in parallel, depending on the number of the
machine's CPU cores.
"""

from __future__ import print_function
import argparse
from multiprocessing import Pool
import os
import subprocess
import tempfile

from utils import *


def verify_file(alluxio_file):
    global args
    local_file = tempfile.mkstemp()[1]
    alluxio = os.path.join(args.home, 'bin', 'alluxio')
    cat_cmd = '%s fs cat %s > %s' % (alluxio, alluxio_file, local_file)
    subprocess.check_call(cat_cmd, shell=True)
    diff_cmd = 'diff %s %s' % (local_file, args.src)
    print('comparing Alluxio file %s to local file %s ... ' % (alluxio_file, args.src))
    subprocess.check_output(diff_cmd, shell=True)
    print('verified that %s was written correctly' % alluxio_file)


def main():
    global args
    alluxio_files = []
    for iteration in range(args.iteration):
        for process in range(args.nprocess):
            file = alluxio_path(args.dst, iteration, args.node, process)
            alluxio_files.append(file)

    pool = Pool(args.process_pool_size)
    pool.map(verify_file, alluxio_files)
    print('Success!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Verify that files are written correctly')
    parser.add_argument('--home', required=True,
                        help='path to Alluxio home directory')
    parser.add_argument('--src', default='data/5mb.txt',
                        help='path to the local file source')
    parser.add_argument('--dst', default='/alluxio-py-test',
                        help='path to the root directory for all written files')
    parser.add_argument('--node', required=True,
                        help='a unique identifier of this node')
    parser.add_argument('--iteration', type=int, required=True,
                        help='number of iterations of the write task to be verified')
    parser.add_argument('--nprocess', type=int, required=True,
                        help='number of processes of the write task to be verified')
    parser.add_argument('--process-pool-size', type=int, default=4,
                        help='number of concurrent processes for verifying the files')
    args = parser.parse_args()

    main()

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script verifies that all files under the {dst} directory is the same as the {src} file.
The file names are {ID}, where ID starts from 0 to {n}.
It uses the Alluxio shell to cat the file to a temporary lcoal file, and uses diff to compare it with the {src}.
Multiple files may be verified in parallel, depending on the machine's CPU cores.
"""

from __future__ import print_function
import argparse
from multiprocessing import Pool
import os
import subprocess
import tempfile


def verify_file(file_id):
    local_file = tempfile.mkstemp()[1]
    alluxio = os.path.join(args.home, 'bin', 'alluxio')
    alluxio_file = os.path.join(args.dst, args.node, str(file_id))
    cat_cmd = '%s fs cat %s > %s' % (alluxio, alluxio_file, local_file)
    subprocess.check_call(cat_cmd, shell=True)
    diff_cmd = 'diff %s %s' % (local_file, args.src)
    print('comparing Alluxio file %s to local file %s ... ' % (alluxio_file, args.src))
    subprocess.check_output(diff_cmd, shell=True)
    print('verified that %s is written correctly' % alluxio_file)


def main(args):
    pool = Pool(4)
    pool.map(verify_file, range(args.nfile))
    print('Success!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Verify that files are written correctly')
    parser.add_argument('--home', required=True,
                        help='path to Alluxio home directory')
    parser.add_argument('--src', default='data/5mb.txt',
                        help='path to the local file source')
    parser.add_argument('--dst', default='/alluxio-py-test',
                        help='path to the root directory for all written file')
    parser.add_argument('--node', required=True,
                        help='a unique identification of this node')
    parser.add_argument('--nfile', type=int, required=True,
                        help='number of expected files')
    args = parser.parse_args()

    main(args)

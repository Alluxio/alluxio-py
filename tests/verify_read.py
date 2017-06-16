#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script verifies that all files under the {dst} directory are the same as the {src} file.
The file names are {ID}, where ID starts from 0 to {n}.
It uses the Alluxio shell to cat the {src} file to a temporary lcoal file, and uses diff to
compare it with the local files.
Multiple files may be verified in parallel, depending on the machine's CPU cores.
"""

from __future__ import print_function
import argparse
from multiprocessing import Pool
import os
import subprocess
import tempfile


def source(args, file_id):
    if args.node:
        return os.path.join(args.src, args.node, str(file_id))
    return args.src


def verify_file(file_id):
    # Cat the Alluxio source file to a temporary file.
    tmp = tempfile.mkstemp()[1]
    alluxio = os.path.join(args.home, 'bin', 'alluxio')
    alluxio_file = source(args, file_id)
    cat_cmd = '%s fs cat %s > %s' % (alluxio, alluxio_file, tmp)
    subprocess.check_call(cat_cmd, shell=True)

    # Diff between the file read from Alluxio and the source file in the local filesystem.
    local_file = os.path.join(args.dst, str(file_id))
    print('comparing Alluxio file %s to local file %s ... ' % (alluxio_file, local_file))
    diff_cmd = 'diff %s %s' % (tmp, local_file)
    subprocess.check_output(diff_cmd, shell=True)
    print('verified that %s is read correctly', alluxio_file)


def main(args):
    pool = Pool(4)
    pool.map(verify_file, range(args.nfile))
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
    parser.add_argument('--dst', required=True,
                        help='path to the directory for all the data written to the local filesystem')
    parser.add_argument('--node', help='a unique identification of this node, if this is not set, \
                        --src must be a path to an Alluxio file')
    parser.add_argument('--nfile', type=int, required=True, help='number of expected files')
    args = parser.parse_args()

    main(args)

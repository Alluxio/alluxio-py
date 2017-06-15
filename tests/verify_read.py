#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script verifies that all files under the {root} directory is the same as the {src} file.
The file names are expected to be in the format {ID}.txt, where ID starts from 0 to {n}.
It uses the Alluxio shell to cat the {src} file to a temporary lcoal file, and uses diff to
compare it with the local files.
Multiple files may be verified in parallel, depending on the machine's CPU cores.
"""

import argparse
from multiprocessing import Pool
import os
import subprocess
import tempfile


parser = argparse.ArgumentParser(
    description='Verify that files are read correctly')
parser.add_argument('--home', required=True,
                    help='path to Alluxio home directory')
parser.add_argument('--root', required=True,
                    help='root directory for all the data written to the local filesystem')
parser.add_argument('--src', required=True,
                    help='path to the Alluxio file source')
parser.add_argument('--nfile', type=int, required=True,
                    help='number of expected files')
args = parser.parse_args()


# Cat the Alluxio source file to a temporary file.
tmp = tempfile.mkstemp()[1]
alluxio = os.path.join(args.home, 'bin', 'alluxio')
cat_cmd = '%s fs cat %s > %s' % (alluxio, args.src, tmp)
subprocess.check_call(cat_cmd, shell=True)


def verify_file(file_id):
    local_file = os.path.join(args.root, '%d.txt' % file_id)
    print 'verifying %s' % local_file
    diff_cmd = 'diff %s %s' % (tmp, local_file)
    subprocess.check_output(diff_cmd, shell=True)
    print 'verified %s' % local_file


pool = Pool(4)
pool.map(verify_file, range(args.nfile))

print 'Succeed!'

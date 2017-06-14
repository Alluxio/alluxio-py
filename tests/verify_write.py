#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script verifies all files under the {root} directory is the same as the {src} file.
The file names are expected to be in the format {ID}.txt, where ID starts from 0 to {n}.
It uses the Alluxio shell to cat the file to a temporary lcoal file, and uses diff to compare it with the {src}.
"""

import argparse
import os
import subprocess
import tempfile


parser = argparse.ArgumentParser(description='Verify that files are written correctly')
parser.add_argument('--home', required=True, help='path to Alluxio home directory')
parser.add_argument('--root', default='/alluxio-py-test', help='root directory for all the data written to Alluxio')
parser.add_argument('--src', default='data/5mb.txt', help='path to the local file source')
parser.add_argument('--nfile', type=int, required=True, help='number of expected files')
args = parser.parse_args()

for i in xrange(args.nfile):
	tmp = tempfile.mkstemp()[1]
	alluxio = os.path.join(args.home, 'bin', 'alluxio')
	cat_cmd = '%s fs cat %s > %s' % (alluxio, os.path.join(args.root, '%d.txt' % i), tmp)
	subprocess.check_call(cat_cmd, shell=True)
	diff_cmd = 'diff %s %s' % (tmp, args.src)
	subprocess.check_output(diff_cmd, shell=True)

print 'Succeed!'

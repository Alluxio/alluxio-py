#!/bin/bash
#
# This script downloads a prepared 5MB data from Amazon S3, and saves it to data/5mb.txt.

mkdir -p data
wget https://s3-us-west-2.amazonaws.com/alluxio-demo/data/5mb.txt -O data/5mb.txt

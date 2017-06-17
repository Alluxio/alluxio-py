# Overview

The test scripts in this directory are for stress testing of the Alluxio python
client API. The scripts can be run on multiple nodes, on each node, multiple
python processes can be run concurrently to write files to or read files from
Alluxio in parallel. There are also scripts to verify that the files written to
Alluxio or the files read from Alluxio are expected.


# Parallel Write

There are multiple nodes, each node runs multiple python client processes,
each process writes a file to Alluxio with the filename in the format
`{dst}/{nid}/{cid}`.
`dst` is the root directory containing all written files, it should be the same
for all python processes on all nodes.
`nid` is short for node ID, each node has a unique ID specified by user through
`--node` option.
`cid` is short for client ID, on each node, each python client process is
assigned a client ID, consecutively starting from 0 to {nprocess}.
So a python client process can be uniquely identified as `(nid, cid)`, which
means it runs on the node with ID nid, and it has client ID cid.

Example:

The following command runs 20 iterations of the parallel write task.
In each iteration, 2 python processes are run concurrently.
Process 0 writes the local file data/5mb.txt to /alluxio-py-test/1/0 in Alluxio.
Process 1 writes the local file data/5mb.txt to /alluxio-py-test/1/1 in Alluxio.

```bash
./parallel_write.py \
	--iteration=20 \
	--nprocess=2 \
	--host=<Alluxio proxy server's hostname> \
	--port=<Alluxio proxy server's web port> \
	--src=data/5mb.txt \
	--dst=/alluxio-py-test \
	--node=1
```


# Parallel Read

There are two modes for testing parallel read. The first is to read the same
file to stress testing the performance and stability of the file metadata
server and data server. The second is to read different files from different
nodes to stress testing data transfer over a conjested network.

## Mode 1

All python processes on the same node read the same file from Alluxio.
In this mode, do not specify `--node`, specify `--src` to the file you want
to read.

Example:

The following command runs 20 iterations of the parallel read task.
In each iteration, 2 python processes are run concurrently.
Each python process reads the same file /alluxio-py-test/test.txt from Alluxio,
and writes the file to local directory /tmp/alluxio-py-test.
Process 0 writes the file to /tmp/alluxio-py-test/0.
Process 1 writes the file to /tmp/alluxio-py-test/1.

```bash
./parallel_read.py \
	--iteration=20 \
	--nprocess=2 \
	--host=<Alluxio proxy server's hostname> \
	--port=<Alluxio proxy server's web port> \
	--src=/alluxio-py-test/test.txt \
	--dst=/tmp/alluxio-py-test
```


## Mode 2

Python process `(nid, cid)` reads the file written by python process `cid`
on a node specified by `--node` which should be different from `nid`.
So the python processes do not read the files written by themselves.

Example:

The following command is run on node 1.
It runs 20 iterations of the parallel read task.
In each iteration, 2 python processes are run concurrently.
Process 0 reads /alluxio-py-test/2/0 from Alluxio and writes it to the local
file /tmp/aluxio-py-test/0.
Process 1 reads /alluxio-py-test/2/1 from Alluxio and writes it to the local
file /tmp/alluxio-py-test/1.

```bash
./parallel_read.py \
	--iteration=20 \
	--nprocess=2 \
	--host=<Alluxio proxy server's hostname> \
	--port=<Alluxio proxy server's web port> \
	--src=/alluxio-py-test \
	--dst=/tmp/alluxio-py-test
	--node=2
```


# Verification

For ease of verifying the correctness of the writes and reads, all files
have the same content as a file in the local filesystem, by default, the file
is data/5mb.txt which can be downloaded by `get_data.sh`.

The reads and writes can be verified by `verify_read.py` and `verify_write.py`
respectively.

Example:

The following command verifies that the Alluxio files /alluxio-py-test/1/0 and
/alluxio-py-test/1/1 are the same as the local file data/5mb.txt.

```bash
./verify_write.py \
	--home=<alluxio installation directory> \
	--src=data/5mb.txt \
	--dst=/alluxio-py-test \
	--nfile=2 \
	--node=1
```

The following command verifies that the local files /tmp/alluxio-py-test/0 and
/tmp/alluxio-py-test/1 that are read from Alluxio are the same as the Alluxio
files /alluxio-py-test/2/0 and /alluxio-py-test/2/1.

```bash
./verify_read.py \
	--home=<alluxio installation directory> \
	--src=/alluxio-py-test \
	--dst=/tmp/alluxio-py-test \
	--nfile=2 \
	--node=2
```

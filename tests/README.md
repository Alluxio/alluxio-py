# Overview

The test scripts in this directory stress tests the Alluxio python client API.
The scripts can be run on multiple nodes where each node will launch multiple
processes to concurrently read from or write to Alluxio. Verification scripts
are provided to validate the operations performed.


# Parallel Write

There are multiple nodes, each node runs multiple python client processes,
each process launched will write a file to Alluxio with filename
`{dst}/node_{node_id}/process_{process_id}`.
`dst` is the root directory containing all written files; this should be set to
the same value for each client on all nodes.
`node_id` is the unique ID specified by the user through `--node` option for
each node.
`process_id` is the consecutive ID from 0 to `{nprocess}` assigned to python
processes on each node.

Example:

The following command runs 20 iterations of the parallel write task.
In each iteration, 2 python processes are run concurrently.
Process 0 writes the local file data/5mb.txt to /alluxio-py-test/node_1/process_0 in Alluxio.
Process 1 writes the local file data/5mb.txt to /alluxio-py-test/node_1/process_1 in Alluxio.
The final iteration leaves data behind so that it can be read by the parallel read test.

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

There are two modes for testing parallel read: local and remote.
Local mode reads the same file multiple times in parallel to stress test the
performance and stability of the file metadata server and data server.
Remote mode reads files written by a different node to stress test data transfer
over a congested network.

## Local Mode

All python processes on the same node read the same file from Alluxio.
In this mode, specify `--src` as the file to read and do not specify `--node`.


Example:

The following command runs 20 iterations of the parallel read task.
In each iteration, 2 python processes are run concurrently.
Each python process reads the same file /alluxio-py-test/test.txt from Alluxio,
and writes the file to local directory /tmp/alluxio-py-test.
Process 0 writes the file to /tmp/alluxio-py-test/process_0.
Process 1 writes the file to /tmp/alluxio-py-test/process_1.

```bash
./parallel_read.py \
	--iteration=20 \
	--nprocess=2 \
	--host=<Alluxio proxy server's hostname> \
	--port=<Alluxio proxy server's web port> \
	--src=/alluxio-py-test/test.txt \
	--dst=/tmp/alluxio-py-test
```


## Remote Mode

Each python process on the same node reads the file written by the corresponding
process on a separate node, specified by `--node`.

Example:

The following command is run on node 1.
It runs 20 iterations of the parallel read task.
In each iteration, 2 python processes are run concurrently.
Process 0 reads /alluxio-py-test/node_2/process_0 from Alluxio and writes it to the local
file /tmp/aluxio-py-test/process_0.
Process 1 reads /alluxio-py-test/node_2/process_1 from Alluxio and writes it to the local
file /tmp/alluxio-py-test/process_1.

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
have the same content as a file in the local filesystem. By default, the file
is data/5mb.txt which can be downloaded by `get_data.sh`.

The reads and writes can be verified by `verify_read.py` and `verify_write.py`
respectively.

Example:

The following command verifies that the Alluxio files
/alluxio-py-test/node_1/process_0 and /alluxio-py-test/node_1/process_1
are the same as the local file data/5mb.txt.

```bash
./verify_write.py \
	--home=<alluxio installation directory> \
	--src=data/5mb.txt \
	--dst=/alluxio-py-test \
	--nfiles=2 \
	--node=1
```

The following command verifies that the local files /tmp/alluxio-py-test/0 and
/tmp/alluxio-py-test/process_1 that are read from Alluxio are the same as the Alluxio
files /alluxio-py-test/node_2/process_0 and /alluxio-py-test/node_2/process_1.

```bash
./verify_read.py \
	--home=<alluxio installation directory> \
	--src=/alluxio-py-test \
	--dst=/tmp/alluxio-py-test \
	--nfiles=2 \
	--node=2
```

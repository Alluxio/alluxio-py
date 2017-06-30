# Overview

The test scripts in this directory stress tests the Alluxio python client API.
The scripts can be run on multiple nodes where each node will launch multiple
processes to concurrently read from or write to Alluxio. Verification scripts
are provided to validate the operations performed.


# Parallel Write

There are multiple nodes, each node runs multiple python client processes,
each process launched will write a file to Alluxio with filename
`{dst}/iteration_{iteration_id}/node_{node_id}/process_{process_id}`.
`dst` is the root directory containing all written files; this should be set to
the same value for each client on all nodes.
`iteration_id` is the number of iteration of running the parallel write.
`node_id` is the unique ID specified by the user through `--node` option for
each node.
`process_id` is the consecutive ID from 0 to `{nprocess}` assigned to python
processes on each node.

Example:

The following command runs 20 iterations of the parallel write task.
In each iteration, 2 python processes are run concurrently.
For example, during the first iteration,
process 0 writes the local file data/5mb.txt to /alluxio-py-test/iteration_0/node_1/process_0 in Alluxio;
process 1 writes the local file data/5mb.txt to /alluxio-py-test/iteration_0/node_1/process_1 in Alluxio.

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
and verifies the file against a local file specified by --expected.

```bash
./parallel_read.py \
	--iteration=20 \
	--nprocess=2 \
	--host=<Alluxio proxy server's hostname> \
	--port=<Alluxio proxy server's web port> \
	--src=/alluxio-py-test/test.txt \
	--expected=data/5mb.txt
```


## Remote Mode

Each python process on the same node reads the file written by the corresponding
process on a separate node, specified by `--node`.

Example:

The following command is run on node 1.
It runs 20 iterations of the parallel read task.
In each iteration, 2 python processes are run concurrently.
For example, during the first iteration,
process 0 reads /alluxio-py-test/iteration_0/node_2/process_0 from Alluxio;
process 1 reads /alluxio-py-test/iteration_0/node_2/process_1 from Alluxio.
Each process verifies the read file against the local file specified by --expected.

```bash
./parallel_read.py \
	--iteration=20 \
	--nprocess=2 \
	--host=<Alluxio proxy server's hostname> \
	--port=<Alluxio proxy server's web port> \
	--src=/alluxio-py-test \
	--expected=data/5mb.txt
	--node=2
```


# Verification

For ease of verifying the correctness of the writes, all files
have the same content as a file in the local filesystem. By default, the file
is data/5mb.txt which can be downloaded by `get_data.sh`.

The writes can be verified by `verify_write.py`.

Example:

The following command verifies that the Alluxio files
/alluxio-py-test/iteration_{0..19}/node_1/process_{0..1} are the same as the local file data/5mb.txt
({a..b} should be expanded to the consequent numbers between a and b, including both side).

```bash
./verify_write.py \
	--home=<alluxio installation directory> \
	--src=data/5mb.txt \
	--dst=/alluxio-py-test \
	--iteration=20 \
	--nprocess=2 \
	--node=1
```


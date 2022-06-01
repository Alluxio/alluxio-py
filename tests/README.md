# Integration tests

<!-- TOC -->

- [Alluxio example cluster](#alluxio-example-cluster)
- [Functional tests](#functional-tests)
- [Stress tests](#stress-tests)

<!-- /TOC -->


## Alluxio example cluster

Can be used as a playground and for functional integration testing

Requires [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/).

Start:

```
docker-compose up --build -d alluxio-proxy
```

Check:

```
proxy_host=<your docker machine host>
port=39999

curl -X POST http://$proxy_host:$port/api/v1/paths//my/create-directory
curl -X POST http://$proxy_host:$port/api/v1/paths///list-status
```

Tear down:

```
docker-compose down -v
```

## Functional tests

```
ALLUXIO_HOST=<your docker machine host> ALLUXIO_PORT=39999 pytest -m it
```

## Stress tests

Test scripts described below run stress tests using the Alluxio python client API.
The scripts can be run on multiple nodes where each node will launch multiple
processes to concurrently read from or write to Alluxio.


## Parallel Write

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


## Parallel Read

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

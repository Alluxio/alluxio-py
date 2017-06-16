# Parallel Write

There are multiple client nodes, each node has multiple python client processes,
each process writes a file to Alluxio, with the filename in the format
`{dst}/{nid}/{cid}`, nid is short for node ID and cid is short for client ID.
`dst` is the `--dst` option passed to `parallel_write.py`, this should be the same
for all python client processes on all nodes.
`nid` is the `--node` option passed to `parallel_write.py`,
it should be the same for all python clients on the same node.
On each node, each python client is assigned a client ID `cid`, starting from 0,
so a client can be uniquely identified as the tuple `(nid, cid)`.


# Parallel Read

## Mode 1

All python client processes on the same client node read the same file from Alluxio.
In this mode, do not specify `--node`, specify `--src` to the file you want
to read.

## Mode 2

Python client `(nid, cid)` will read the file written by python client `cid`
on a node specified by `--node` which should be different from `nid`,
so the clients do not read the files written by themselves. In this mode, specify
`--src` to the {dst} set in the parallel write section.


# Verification

For ease of verifying the correctness of the writes and reads, all files
have the same content as a file in the local filesystem, by default, the file
is data/5mb.txt which can be downloaded by `get_data.sh`.

The reads and writes can be verified by `verify_read.py` and `verify_write.py`
respectively, see `verify_read.py -h` and `verify_write.py -h` for details.


# Example

Two nodes, m1 and m2.
On each node, there are two python clients, c1 and c2.

Do parallel writes:

* on node m1: `./parallel_write.py --src=data/5mb.txt --dst=/aluxio-py-test --node=m1`,
* on node m2: `./parallel_write.py --src=data/5mb.txt --dst=/aluxio-py-test --node=m2`,

then there will be four files in Alluxio:

* /alluxio-py-test/m1/c1
* /alluxio-py-test/m1/c2
* /alluxio-py-test/m2/c1
* /alluxio-py-test/m2/c2

Verify the writes:

* on node m1: `./verify_write.py --home=<alluxio installation directory> --src=data/5mb.txt --dst=/alluxio-py-test --nfile=2 --node=m1`.
* on node m2: `./verify_write.py --home=<alluxio installation directory> --src=data/5mb.txt --dst=/alluxio-py-test --nfile=2 --node=m2`.

Do parallel reads:

* on node m1: `./parallel_read.py --src=/alluxio-py-test --dst=/tmp/alluxio-py-test --node=m2`,
* on node m2: `./parallel_read.py --src=/alluxio-py-test --dst=/tmp/alluxio-py-test --node=m1`,

note that for node `m1`, the `--node` is set to 2 so that clients on `m1` read
files written by clients on `m2`, same note for node `m2`.

Then on each node's local /tmp/alluxio-py-test, there will be two files:

* c1
* c2

Verify the reads:

* on node m1: `./verify_read.py --home=<alluxio installation directory> --src=/alluxio-py-test --dst=/tmp/alluxio-py-test --nfile=2 --node=m2`.
* on node m2: `./verify_read.py --home=<alluxio installation directory> --src=/alluxio-py-test --dst=/tmp/alluxio-py-test --nfile=2 --node=m1`.

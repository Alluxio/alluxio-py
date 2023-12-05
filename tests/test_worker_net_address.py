import pytest

from alluxio.worker_ring import WorkerNetAddress


def test_worker_net_address_from_worker_info():
    worker_info = b'{"WorkerNetAddress": {"Host": "localhost"}}'
    result = WorkerNetAddress.from_worker_info(worker_info)
    assert result.host == "localhost", "The host should be localhost"

    invalid_worker_info = b'{"invalid_json": {}'
    with pytest.raises(ValueError) as err:
        WorkerNetAddress.from_worker_info(invalid_worker_info)
    assert (
        "Provided worker_info is not a valid JSON string" in err.value.args[0]
    )

    non_bytes_worker_info = '{"WorkerNetAddress": {"Host": "localhost"}}'
    with pytest.raises(AttributeError) as err:
        WorkerNetAddress.from_worker_info(non_bytes_worker_info)
    assert (
        "Provided worker_info must be a bytes-like object" in err.value.args[0]
    )


def test_worker_net_address_from_worker_hosts():
    worker_hosts = "host1,host2,host3"
    result = WorkerNetAddress.from_worker_hosts(worker_hosts)
    assert len(result) == 3, "The length of worker list should be 3"


def test_worker_net_address_dump_main_info():
    worker_address = WorkerNetAddress(host="host", rpc_port=1234)
    result = worker_address.dump_main_info()
    print(result)
    assert "host" in result, "host should be included in the dump info"
    assert (
        "1234" in result
    ), "rpc port 1234 should be included in the dump info"
    assert (
        str(WorkerNetAddress.DEFAULT_DATA_PORT) in result
    ), "default data port should be included in the dump info"

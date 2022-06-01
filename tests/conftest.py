import os
import typing

import pytest

from alluxio import Client
from alluxio.option import Delete


class TestConf(typing.NamedTuple(
    "TestConf",
    [("host", str), ("port", int), ("test_dir", str)])
):
    pass


@pytest.fixture(scope="session")
def conf():
    return TestConf(
        host=os.environ["ALLUXIO_HOST"],
        port=int(os.environ["ALLUXIO_PORT"]),
        test_dir="/test"
    )


@pytest.fixture(scope="session")
def client(conf):
    return Client(conf.host, conf.port)


@pytest.fixture(scope="class")
def fs_tree(client, conf):
    client.create_directory(conf.test_dir)
    client.create_directory(conf.test_dir + "/path")
    client.create_directory(conf.test_dir + "/path/bar")

    yield

    opt = Delete(recursive=True)
    client.delete(conf.test_dir, opt)

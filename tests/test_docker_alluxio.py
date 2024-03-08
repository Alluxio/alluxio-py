from tests.conftest import TEST_ROOT


def test_simple(alluxio_client):
    alluxio_client.listdir(TEST_ROOT)  # no error


def test_simple_etcd(etcd_alluxio_client):
    etcd_alluxio_client.listdir(TEST_ROOT)  # no error

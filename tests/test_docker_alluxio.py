from tests.conftest import TEST_ROOT


def test_simple(alluxio_client):
    alluxio_client.listdir(TEST_ROOT)  # no error

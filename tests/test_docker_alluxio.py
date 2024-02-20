from tests.conftest import TEST_ROOT


def test_simple(fs):
    fs.listdir(TEST_ROOT)  # no error

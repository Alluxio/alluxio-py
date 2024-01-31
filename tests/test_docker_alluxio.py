from tests.conftest import TEST_ROOT


def test_simple(fs):
    result = fs.listdir(TEST_ROOT)  # no error
    print(result)

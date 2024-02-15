import pytest

from tests.conftest import TEST_ROOT


@pytest.mark.skip
def test_simple(fs):
    fs.listdir(TEST_ROOT)  # no error

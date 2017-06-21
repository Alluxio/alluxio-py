from alluxio.common import String

from util import assert_string_subclass


def test_string():
    name = 'name'
    assert_string_subclass(String(name), name)

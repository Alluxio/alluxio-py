#! -*- coding: utf-8 -*-
"""
Commonly used classes throughout the entire Alluxio Python interface.
"""
import json
import sys

from six import reraise


class _JsonEncodable(object):
    """Base class for all classes that can be encoded into json."""

    def json(self):
        """Convert the object into a python dict which can be encoded into a json string."""

    def __repr__(self):
        """Return the json string representation of this object."""
        return json.dumps(self.json())


class _JsonDecodable(object):  # pylint: disable=too-few-public-methods
    """Base class for all classes that can be decoded from json."""

    @classmethod
    def from_json(cls, obj):
        """Return an instance of cls decoded from the json string obj.

        The implementation of in this base class just returns cls(str(obj)).

        Args:
            cls: This class.
            obj: The json string to be decoded.

        Returns:
                An instance of cls decoded from obj.
        """

        return cls(str(obj))


class String(_JsonEncodable, _JsonDecodable):
    """Base class for all classes that are type aliases of a str.

    Args:
        name (str): The string alias of the object.

    Examples:
        see :class:`alluxio.wire.Bits`.
    """

    def __init__(self, name=''):
        self.name = name

    def json(self):
        return self.name

    @classmethod
    def from_json(cls, obj):
        return cls(str(obj))


def raise_with_traceback(error_type, message):
    """Raise a new error with the most recent traceback."""

    traceback = sys.exc_info()[2]
    reraise(error_type, error_type(message), traceback)

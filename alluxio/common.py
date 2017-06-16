#! -*- coding: utf-8 -*-
import json


class _JsonEncodable(object):
    """Base class for all classes that can be encoded into json."""

    def json(self):
        """Convert the object into a python dict which can be encoded into a json string."""
        pass

    def __repr__(self):
        """Return the json string representation of this object."""
        return json.dumps(self.json())


class _JsonDecodable(object):
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

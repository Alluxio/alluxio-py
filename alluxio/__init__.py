# -*- coding: utf-8 -*-
"""
The Python Alluxio module relies on an existing Alluxio cluster being set up and properly configured, as well as an
Alluxio Proxy server being set up and working. All work is done via HTTP(S). Please see
https://docs.alluxio.io/os/user/stable/en/api/FS-API.html#rest-api for details on configuring this essential part of
your environment.
"""
from .client import Client  # noqa: F401
from . import option  # noqa: F401
from . import wire  # noqa: F401

__version__ = '0.1.4'

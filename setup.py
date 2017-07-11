#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

import alluxio


setup(
    name='alluxio',
    version=alluxio.__version__,
    description='Alluxio python client',
    long_description='Alluxio python client based on REST API',
    url='https://github.com/TachyonNexus/alluxio-py',
    author='Alluxio, Inc',
    packages=find_packages(exclude=['docs', 'tests']),
    install_requires=['six', 'requests'],
    tests_require=['pytest', 'pytest-cov']
)

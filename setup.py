#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

import os


def get_version():
    lines = open(os.path.join('alluxio', '__init__.py')).readlines()
    vlines = [x for x in lines if '__version__' in x]
    if len(vlines) != 1:
        print(vlines)
        raise Exception('No version found or too many versions found')
    return vlines[0].replace('__version__', '').replace('=', '').strip(" '\"\n\t")


setup(
    name='alluxio',
    version=get_version(),
    description='Alluxio python client',
    long_description='Alluxio python client based on REST API',
    url='https://github.com/Alluxio/alluxio-py',
    author='Alluxio, Inc',
    license="Apache-2.0",
    packages=find_packages(exclude=['docs', 'tests']),
    install_requires=['six', 'requests'],
    tests_require=['pytest', 'pytest-cov']
)

# Contributing to alluxio-py

Thanks for your interest in **alluxio-py**. The project is to enable data access to Alluxio in Python.

## Getting Started

We recommend you follow the [readme](README.md) first to get familiar with the project. 
Contributors would need to install the Python library and dependencies.

## Code Style

The project leverages [Black](https://github.com/psf/black) as the code formatter and [reorder-python-imports](https://github.com/asottile/reorder_python_imports) to format imports.
Black defaults to 88 characters per line (10% over 80), while this project still uses 80 characters per line. 
We recommend running the following commands before submitting pull requests.

```bash
black [changed-file].py --line-length 79
reorder-python-imports [changed-file].py
```

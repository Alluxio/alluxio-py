# Alluxio Python Client

[![Documentation Status](https://readthedocs.org/projects/alluxio-py/badge/?version=latest)](http://alluxio-py.readthedocs.io/en/latest/?badge=latest)

## Install

```bash
pip install alluxio
```

## Example

See `example.py`.

## Dependencies

* Python 2.7 or later
* requests
* pytest
* tox

```bash
pip install -r requirements.txt
```

## Install for Development

```bash
pip install -e .
```

## Code Style

Follow [pep8](https://www.python.org/dev/peps/pep-0008/) for source code style,
except the restriction for line width.

Check code style by `pep8 --ignore=E501 alluxio/`.

Follow [Google style](http://www.sphinx-doc.org/en/stable/ext/example_google.html)
for docstrings.

## Generate Documentation

```bash
pip install sphinx~=5.0
cd docs
make html
# open docs/_build/html/index.html in your browser
```

## Unit Tests

Unit tests are in `alluxio/tests` directory, they can be run under the `alluxio-py` directory by

```bash
pytest
```

or

```bash
tox
```

See the test coverage report by

```bash
pytest --cov .
```

## Integration Tests

Integration tests are under the `tests` directory, see `tests/README.md` for more details.

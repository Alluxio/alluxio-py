# Alluxio Python Client

## Dependencies

* Python 2.6 or later
* requests

```bash
pip install -r requirements.txt
```

## Code Style

Follow [pep8](https://www.python.org/dev/peps/pep-0008/) for source code style,
except the restriction for line width.

Check code style by `pep8 --ignore=E501 alluxio/`.

Follow [Google style](http://www.sphinx-doc.org/en/stable/ext/example_google.html)
for docstrings.

## Generate Documentation

```bash
pip install 'sphinx<1.4' # the latest sphinx has some format issue in the generated html files
cd docs
make html
# open docs/_build/html/index.html in your browser
```

from setuptools import find_packages
from setuptools import setup

setup(
    name="alluxio-python-library",
    version="0.1",
    packages=find_packages(exclude=["tests", "tests.*"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "humanfriendly",
        "requests",
        "etcd3",
        "mmh3",
        "sortedcontainers",
    ],
    extras_require={"tests": ["pytest"]},
    python_requires=">=3.8",
)

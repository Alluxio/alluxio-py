from setuptools import find_packages
from setuptools import setup

setup(
    name="alluxio",
    version="1.0.0",
    description="Alluxio Python library 1.0.0 provides API to interact with Alluxio servers.",
    url="https://github.com/Alluxio/alluxio-py",
    packages=find_packages(exclude=["tests", "tests.*"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "aiohttp",
        "decorator",
        "humanfriendly",
        "requests",
        "etcd3",
        "mmh3",
        "sortedcontainers",
        "protobuf>=3.20.0,<3.21.0",
    ],
    extras_require={"tests": ["pytest", "pytest-aiohttp"]},
    python_requires=">=3.8",
)

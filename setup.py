from setuptools import find_packages
from setuptools import setup

setup(
    name="alluxio",
    version="0.1.0",
    description="Alluxio Python SDK provides Alluxio access implementation.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Alluxio/alluxio-py",
    packages=['alluxio'],
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
    extras_require={
        "tests": [
            "pytest",
            "pytest-aiohttp",
            "ray",
            "pyarrow",
        ]
    },
    python_requires=">=3.8",
    maintainer="Alluxio",
    entry_points={
        "fsspec.specs": [
            "alluxio=alluxiofs.AlluxioClient",
        ],
    },
)

from setuptools import find_packages
from setuptools import setup

setup(
    name="alluxio_posix",
    version="0.1.0",
    packages=find_packages(),
    license="MIT",
    description="Alluxio POSIX Python SDK",
    author="lzq",
    author_email="liuzq0909@163.com",
    data_files=[("config", ["config/ufs_config.yaml"])],
    include_package_data=True,
    zip_safe=False,
)

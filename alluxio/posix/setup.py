from setuptools import setup, find_packages

setup(
    name='alluxio_posix',
    version='0.1.0',
    packages=find_packages(),
    license='MIT',
    description='Alluxio POSIX Python SDK',
    author='lzq',
    author_email='liuzq0909@163.com',
    data_files=[
        ('config', ['config/ufs_config.yaml'])  # 指定配置文件所在路径
    ],
    include_package_data=True,
    zip_safe=False
)
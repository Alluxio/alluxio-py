import logging
import re
import threading

import fsspec

from alluxio.posix.config import ConfigManager
from alluxio.posix.const import Constants
from alluxio.posix.exception import UnsupportedDelegateFileSystemError
from alluxio.posix.ufs.alluxio import ALLUXIO_BACKUP_FS
from alluxio.posix.ufs.alluxio import ALLUXIO_ENABLE
from alluxio.posix.ufs.alluxio import ALLUXIO_ETCD_ENABLE
from alluxio.posix.ufs.alluxio import ALLUXIO_ETCD_HOST
from alluxio.posix.ufs.oss import OSS_ACCESS_KEY_ID
from alluxio.posix.ufs.oss import OSS_ACCESS_KEY_SECRET
from alluxio.posix.ufs.oss import OSS_ENDPOINT


class DelegateFileSystem:
    instance = None

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.filesystem_storage = FSStorage()
        self.filesystem_storage.data = {}
        self.enableFileSystems = [
            Constants.OSS_FILESYSTEM_TYPE,
            Constants.ALLUXIO_FILESYSTEM_TYPE,
            Constants.S3_FILESYSTEM_TYPE,
        ]
        self.__init__file__system()
        DelegateFileSystem.instance = self

    def __create__file__system(self, fs_name: str):
        config = self.config_manager.get_config(fs_name)
        if fs_name not in self.enableFileSystems:
            raise UnsupportedDelegateFileSystemError(
                f"Unsupported file system: {fs_name}"
            )
        if config[ALLUXIO_ENABLE]:
            fs_name = (
                Constants.ALLUXIO_FILESYSTEM_TYPE
                + Constants.ALLUXIO_SEP_SIGN
                + fs_name
                + Constants.ALLUXIO_SEP_SIGN
                + config[Constants.BUCKET_NAME]
            )
            if config.get(ALLUXIO_ETCD_ENABLE):
                self.filesystem_storage.fs[fs_name] = fsspec.filesystem(
                    Constants.ALLUXIO_FILESYSTEM_TYPE,
                    etcd_hosts=config[ALLUXIO_ETCD_HOST],
                    etcd_port=2379,
                    target_protocol=config[ALLUXIO_BACKUP_FS],
                )
                return self.filesystem_storage.fs[fs_name]
            else:
                logging.error(
                    "Failed to create Alluxio filesystem, using the default %s filesystem.",
                    fs_name,
                )
        if fs_name == Constants.OSS_FILESYSTEM_TYPE:
            self.filesystem_storage.fs[fs_name] = fsspec.filesystem(
                Constants.OSS_FILESYSTEM_TYPE,
                key=config[OSS_ACCESS_KEY_ID],
                secret=config[OSS_ACCESS_KEY_SECRET],
                endpoint=config[OSS_ENDPOINT],
            )
            return self.filesystem_storage.fs[fs_name]
        elif fs_name == Constants.S3_FILESYSTEM_TYPE:
            # todo：新增s3FileSystem
            raise NotImplementedError

        return None

    def get_file_system(self, path: str):
        fs_name, bucket = self.__parse__url(path)
        if fs_name == Constants.LOCAL_FILESYSTEM_TYPE:
            return None
        config = self.config_manager.get_config(fs_name)
        if config[ALLUXIO_ENABLE]:
            fs_name = (
                Constants.ALLUXIO_FILESYSTEM_TYPE
                + Constants.ALLUXIO_SEP_SIGN
                + fs_name
                + Constants.ALLUXIO_SEP_SIGN
                + config[Constants.BUCKET_NAME]
            )
        if hasattr(self.filesystem_storage, fs_name):
            return self.filesystem_storage.fs[fs_name]
        else:
            self.__create__file__system(fs_name)
            return self.filesystem_storage.fs[fs_name]

    def __init__file__system(self):
        fs_list = self.config_manager.get_config_fs_list()
        for fs_name in fs_list:
            self.__create__file__system(fs_name)

    def __parse__url(self, path: str):
        # parse the schema and bucket name in filepath
        if (type(path) is not str) or (path.startswith("/")):
            return Constants.LOCAL_FILESYSTEM_TYPE, None
        pattern = re.compile(r"^(\w+)://([^/]+)/.*")
        match = pattern.match(path)
        if match:
            fs_name, bucket_name = match.groups()
            # Check whether the file system corresponding to the path is supported
            if fs_name.lower() in self.enableFileSystems:
                return fs_name, bucket_name
            else:
                raise UnsupportedDelegateFileSystemError(
                    f"Unsupported file system: {fs_name}"
                )
        else:
            return Constants.LOCAL_FILESYSTEM_TYPE, None


class FSStorage(threading.local):
    def __init__(self):
        self.fs = {}

    def __getitem__(self, key):
        return self.fs[key]

    def __setitem__(self, key, value):
        self.fs[key] = value

    def __delitem__(self, key):
        del self.fs[key]

    def __contains__(self, key):
        return key in self.fs

    def get(self, key, default=None):
        return self.fs.get(key, default)

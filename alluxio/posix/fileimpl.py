import logging
import os
import re
import threading
import fsspec

from alluxio.posix.config import ConfigManager
from alluxio.posix.const import Constants
from alluxio.posix.exception import UnsupportedDelegateFileSystemError

local_open = os.path
local_stat = os.stat
local_listdir = os.listdir
local_rename = os.rename
local_close = os.close
local_mkdir = os.mkdir
local_remove = os.remove
local_rmdir = os.rmdir


def open(file: str, mode: str = "r", **kw):
    logging.info("DelegateFileSystem opening file: %s", file)
    instance = DelegateFileSystem.instance
    fs = instance.get_file_system(file)
    if fs:
        try:
            return fs.open(file, mode, **kw)
        except Exception as e:
            logging.error(
                f"Failed to open file by delegateFileSystem with exception:{e}."
                f"Used local filesystem instead.")
            return local_open(file, mode, **kw)
    return local_open(file, mode, **kw)


def stat(path: str, **kw):
    instance = DelegateFileSystem.instance
    fs = instance.get_file_system(path)
    if fs:
        try:
            logging.info("DelegateFileSystem getStatus filemeta: %s", path)
            return fs.stat(path, **kw)
        except Exception as e:
            logging.error(
                f"Failed to stat file by delegateFileSystem with exception:{e}."
                f"Used local filesystem instead.")
            return local_stat(path, **kw)
    logging.info("LocalFileSystem getStatus filemeta: %s", path)
    return local_stat(path, **kw)


def listdir(path: str, **kw):
    instance = DelegateFileSystem.instance
    fs = instance.get_file_system(path)
    if fs:
        try:
            return fs.listdir(path, **kw)
        except Exception as e:
            logging.error(
                f"Failed to list directory by delegateFileSystem with exception: {e}."
                f"Used local filesystem instead.")
            return local_listdir(path, **kw)
    return local_listdir(path, **kw)


def mkdir(path: str, mode=0o777, **kw):
    instance = DelegateFileSystem.instance
    fs = instance.get_file_system(path)
    if fs:
        try:
            return fs.mkdir(path, mode, **kw)
        except Exception as e:
            logging.error(
                f"Failed to make directory by delegateFileSystem with exception: {e}."
                f"Used local filesystem instead.")
            return local_mkdir(path, mode, **kw)
    return local_mkdir(path, mode, **kw)


def rmdir(path: str, **kw):
    instance = DelegateFileSystem.instance
    fs = instance.get_file_system(path)
    if fs:
        try:
            return fs.rmdir(path, **kw)
        except Exception as e:
            logging.error(
                f"Failed to remove directory by delegateFileSystem with exception: {e}."
                f"Used local filesystem instead."
            )
            return local_rmdir(path, **kw)
    return local_rmdir(path, **kw)


def remove(path: str, **kw):
    instance = DelegateFileSystem.instance
    fs = instance.get_file_system(path)
    if fs:
        try:
            return fs.rm(path, **kw)
        except Exception as e:
            logging.error(
                f"Failed to remove file by delegateFileSystem with exception: {e}."
                f"Used local filesystem instead.")
            return local_remove(path, **kw)
    return local_remove(path, **kw)


def rename(src: str, dest: str, **kw):
    instance = DelegateFileSystem.instance
    fs_src = instance.get_file_system(src)
    fs_dest = instance.get_file_system(dest)
    if fs_src and fs_dest and fs_src == fs_dest:
        try:
            return fs_src.rename(src, dest, **kw)
        except Exception as e:
            logging.error(
                f"Failed to rename file by delegateFileSystem with exception: {e}."
                f"Used local filesystem instead.")
            return local_rename(src, dest, **kw)
    logging.error("Source and destination are on different file systems or not supported.")
    return local_rename(src, dest, **kw)


class DelegateFileSystem:
    instance = None

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.filesystem_storage = FSStorage()
        self.filesystem_storage.data = {}
        self.enableFileSystems = [Constants.OSS_FILESYSTEM_TYPE,
                                  Constants.ALLUXIO_FILESYSTEM_TYPE,
                                  Constants.S3_FILESYSTEM_TYPE]
        self.__init__file__system()
        DelegateFileSystem.instance = self

    def __create__file__system(self, fs_name: str):
        config = self.config_manager.get_config(fs_name)
        if fs_name not in self.enableFileSystems:
            raise UnsupportedDelegateFileSystemError(f"Unsupported file system: {fs_name}")
        if config[Constants.ALLUXIO_ENABLE]:
            fs_name = (Constants.ALLUXIO_FILESYSTEM_TYPE + Constants.ALLUXIO_SEP_SIGN + fs_name +
                       Constants.ALLUXIO_SEP_SIGN + config[Constants.BUCKET_NAME])
            if config.get(Constants.ALLUXIO_ETCD_ENABLE):
                self.filesystem_storage.fs[fs_name] = fsspec.filesystem(Constants.ALLUXIO_FILESYSTEM_TYPE,
                                                                        etcd_hosts=config[Constants.ALLUXIO_ETCD_HOST],
                                                                        etcd_port=2379,
                                                                        target_protocol=config[
                                                                            Constants.ALLUXIO_BACKUP_FS])
                return self.filesystem_storage.fs[fs_name]
            else:
                logging.error("Failed to create Alluxio filesystem, using the default %s filesystem.", fs_name)
        if fs_name == Constants.OSS_FILESYSTEM_TYPE:
            self.filesystem_storage.fs[fs_name] = fsspec.filesystem(Constants.OSS_FILESYSTEM_TYPE,
                                                                    key=config[Constants.OSS_ACCESS_KEY_ID],
                                                                    secret=config[Constants.OSS_ACCESS_KEY_SECRET],
                                                                    endpoint=config[Constants.OSS_ENDPOINT])
            return self.filesystem_storage.fs[fs_name]
        elif fs_name == Constants.S3_FILESYSTEM_TYPE:
            # todo：新增s3FileSystem
            raise NotImplementedError

        return None

    def get_file_system(self, path: str):
        fs_name, bucket = self.__parse__url(path)
        config = self.config_manager.get_config(fs_name)
        if fs_name == Constants.LOCAL_FILESYSTEM_TYPE:
            return None
        if config[Constants.ALLUXIO_ENABLE]:
            fs_name = (Constants.ALLUXIO_FILESYSTEM_TYPE + Constants.ALLUXIO_SEP_SIGN + fs_name +
                       Constants.ALLUXIO_SEP_SIGN + config[Constants.BUCKET_NAME])
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
        if (type(path) is not str) or (path.startswith('/')):
            return Constants.LOCAL_FILESYSTEM_TYPE, None
        pattern = re.compile(r'^(\w+)://([^/]+)/.*')
        match = pattern.match(path)
        if match:
            fs_name, bucket_name = match.groups()
            # Check whether the file system corresponding to the path is supported
            if fs_name.lower() in self.enableFileSystems:
                return fs_name, bucket_name
            else:
                raise UnsupportedDelegateFileSystemError(f"Unsupported file system: {fs_name}")
        else:
            return Constants.LOCAL_FILESYSTEM_TYPE, None


class FSStorage(threading.local):
    def __init__(self):
        self.data = {}

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __delitem__(self, key):
        del self.data[key]

    def __contains__(self, key):
        return key in self.data

    def get(self, key, default=None):
        return self.data.get(key, default)

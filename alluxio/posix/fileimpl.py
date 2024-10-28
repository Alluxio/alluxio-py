import logging
import os

from alluxio.posix.delegateFs import DelegateFileSystem

local_open = os.path
local_stat = os.stat
local_listdir = os.listdir
local_rename = os.rename
local_close = os.close
local_mkdir = os.mkdir
local_remove = os.remove
local_rmdir = os.rmdir


def open(file: str, mode: str = "r", **kw):
    logging.debug("DelegateFileSystem opening file: %s", file)
    instance = DelegateFileSystem.instance
    fs = instance.get_file_system(file)
    if fs:
        try:
            return fs.open(file, mode, **kw)
        except Exception as e:
            logging.error(
                f"Failed to open file by delegateFileSystem with exception:{e}."
                f"Used local filesystem instead."
            )
            return local_open(file, mode, **kw)
    return local_open(file, mode, **kw)


def stat(path: str, **kw):
    instance = DelegateFileSystem.instance
    fs = instance.get_file_system(path)
    if fs:
        try:
            logging.debug("DelegateFileSystem getStatus filemeta: %s", path)
            return fs.stat(path, **kw)
        except Exception as e:
            logging.error(
                f"Failed to stat file by delegateFileSystem with exception:{e}."
                f"Used local filesystem instead."
            )
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
                f"Used local filesystem instead."
            )
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
                f"Used local filesystem instead."
            )
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
                f"Used local filesystem instead."
            )
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
                f"Used local filesystem instead."
            )
            return local_rename(src, dest, **kw)
    logging.error(
        "Source and destination are on different file systems or not supported."
    )
    return local_rename(src, dest, **kw)

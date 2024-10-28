import os
from alluxio.posix import fileimpl
from alluxio.posix.config import ConfigManager
from alluxio.posix.delegateFs import DelegateFileSystem

config_manager = ConfigManager()
delegate_fs = DelegateFileSystem(config_manager)

os.stat = fileimpl.stat
os.open = fileimpl.open
os.listdir = fileimpl.listdir
os.rename = fileimpl.rename
os.mkdir = fileimpl.mkdir
os.remove = fileimpl.remove
os.rmdir = fileimpl.rmdir

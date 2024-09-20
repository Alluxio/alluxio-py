import os
from alluxio.posix import fileimpl
config_manager = fileimpl.ConfigManager()
delegate_fs = fileimpl.DelegateFileSystem(config_manager)

os.stat = fileimpl.stat
os.open = fileimpl.open
os.listdir = fileimpl.listdir
os.rename = fileimpl.rename
os.mkdir = fileimpl.mkdir
os.remove = fileimpl.remove
os.rmdir = fileimpl.rmdir



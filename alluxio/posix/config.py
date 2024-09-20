import logging

import yaml
import os

from alluxio.posix.ufs.alluxio import validate_alluxio_config, update_alluxio_config
from alluxio.posix.ufs.oss import validate_oss_config, update_oss_config
from alluxio.posix.const import Constants
from alluxio.posix.exception import ConfigMissingError, ConfigInvalidError


class ConfigManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        self.config_file_path = os.getenv('ALLUXIO_PY_CONFIG_FILE_PATH', 'config/ufs_config.yaml')
        self.config_data = self._load_config()
        self.validation_functions = {
            Constants.OSS_FILESYSTEM_TYPE: validate_oss_config,
            Constants.ALLUXIO_FILESYSTEM_TYPE: validate_alluxio_config
        }

    def _load_config(self):
        if not os.path.exists(self.config_file_path):
            raise FileNotFoundError(f"{self.config_file_path} does not exist.")

        with open(self.config_file_path, 'r', encoding='utf-8') as file:
            try:
                config = yaml.safe_load(file)
                return config
            except yaml.YAMLError as e:
                raise ValueError(f"Error parsing YAML file: {e}")

    def set_config_path(self, new_path):
        self.config_file_path = new_path
        self.config_data = self._load_config()
        print(f"Configuration path updated and config reloaded from {new_path}.")

    def get_config(self, fs_name: str) -> dict:
        try:
            fs_config = self.config_data[fs_name]
            validation_function = self.validation_functions.get(fs_name)
            if validation_function is not None:
                validation_function(fs_config)
            else:
                raise ConfigInvalidError(fs_name, f"No validation function for file system: {fs_name}")
            return fs_config
        except KeyError:
            raise ConfigMissingError(fs_name, "FileSystem Configuration is missing")
        except ValueError as e:
            raise ConfigMissingError(fs_name, str(e))

    def get_config_fs_list(self) -> list:
        return self.config_data.keys()

    def update_config(self, fs_type, key, value):
        if fs_type not in self.get_config_fs_list():
            raise KeyError(f"No configuration available for {fs_type}")
        config_data = self.get_config(fs_type)
        if fs_type == Constants.OSS_FILESYSTEM_TYPE:
            self.config_data[fs_type] = update_oss_config(config_data, key, value)
        elif fs_type == Constants.ALLUXIO_FILESYSTEM_TYPE:
            self.config_data[fs_type] = update_alluxio_config(config_data, key, value)
        elif fs_type == Constants.S3_FILESYSTEM_TYPE:
            raise NotImplementedError()
        else:
            raise ValueError(f"Unsupported file system type: {fs_type}")

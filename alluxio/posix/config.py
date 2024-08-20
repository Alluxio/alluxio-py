import yaml
import os

from alluxio.posix.const import Constants
from alluxio.posix.exception import ConfigMissingError, ConfigInvalidError


class ConfigManager:
    def __init__(self, config_file_path='config/ufs_config.yaml'):
        self.config_file_path = config_file_path
        self.config_data = self._load_config()
        self.validation_functions = {
            Constants.OSS_FILESYSTEM_TYPE: self._validate_oss_config,
            Constants.ALLUXIO_FILESYSTEM_TYPE: self._validate_alluxio_config,
            Constants.S3_FILESYSTEM_TYPE: self._validate_s3_config
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

    @staticmethod
    def _validate_oss_config(config):
        required_keys = [
            Constants.OSS_ACCESS_KEY_ID,
            Constants.OSS_ACCESS_KEY_SECRET,
            Constants.OSS_ENDPOINT,
            Constants.OSS_BUCKET_NAME
        ]

        for key in required_keys:
            if key not in config:
                raise ConfigMissingError(f"Missing required OSS config key: {key}")
            if not config[key]:
                raise ValueError(f"OSS config key '{key}' cannot be empty")

    @staticmethod
    def _validate_alluxio_config(config):
        required_keys = []
        if config.get(Constants.ALLUXIO_ETCD_ENABLE, False):
            # If ALLUXIO_ETCD_ENABLE is True, ALLUXIO_ETCD_HOST must be set
            required_keys.append(Constants.ALLUXIO_ETCD_HOST)
        else:
            # If ALLUXIO_ETCD_ENABLE is False, ALLUXIO_WORKER_HOSTS must be set
            required_keys.append(Constants.ALLUXIO_WORKER_HOSTS)

        if not all(config.get(key) for key in required_keys):
            raise ConfigMissingError(f"The following keys must be set in the configuration: {required_keys}")


    @staticmethod
    def _validate_s3_config(config):
        raise NotImplementedError

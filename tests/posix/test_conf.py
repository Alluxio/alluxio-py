import unittest
from unittest.mock import patch, mock_open

import yaml

from alluxio.posix.config import ConfigManager
from alluxio.posix.const import Constants
from alluxio.posix.exception import ConfigMissingError
from alluxio.posix.ufs.alluxio import ALLUXIO_ETCD_ENABLE, ALLUXIO_ETCD_HOST, update_alluxio_config
from alluxio.posix.ufs.oss import OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, update_oss_config


class TestConfigManager(unittest.TestCase):
    def setUp(self):
        # Prepare a mock configuration
        self.mock_config = {
            Constants.OSS_FILESYSTEM_TYPE: {
                OSS_ACCESS_KEY_ID: "123",
                OSS_ACCESS_KEY_SECRET: "secret",
                OSS_ENDPOINT: "https://oss.example.com",
                Constants.BUCKET_NAME: "example-bucket"
            },
            Constants.ALLUXIO_FILESYSTEM_TYPE: {
                ALLUXIO_ETCD_ENABLE: True,
                ALLUXIO_ETCD_HOST: "https://etcd.example.com"
            }
        }
        # Patch the os.path.exists method to always return True
        patcher = patch('os.path.exists', return_value=True)
        self.addCleanup(patcher.stop)
        self.mock_exists = patcher.start()

        # Patch the open method to use a mock open
        self.mock_file = mock_open(read_data=yaml.dump(self.mock_config))
        patcher = patch('builtins.open', self.mock_file)
        self.addCleanup(patcher.stop)
        self.mock_open = patcher.start()

    def test_load_config_success(self):
        manager = ConfigManager()
        self.assertEqual(manager.config_data, self.mock_config)

    def test_set_config_path(self):
        manager = ConfigManager()
        with self.assertLogs(level='INFO') as log:
            manager.set_config_path('new/path/to/config.yaml')
        self.assertIn('Configuration path updated and config reloaded from new/path/to/config.yaml', log.output[0])

    def test_get_config_valid(self):
        manager = ConfigManager()
        oss_config = manager.get_config(Constants.OSS_FILESYSTEM_TYPE)
        self.assertEqual(oss_config, self.mock_config[Constants.OSS_FILESYSTEM_TYPE])

    def test_get_config_invalid(self):
        manager = ConfigManager()
        with self.assertRaises(ConfigMissingError):
            manager.get_config('nonexistent')

    def test_update_oss_config(self):
        manager = ConfigManager()
        with self.assertLogs(level='INFO') as log:
            update_oss_config(Constants.OSS_FILESYSTEM_TYPE, OSS_ACCESS_KEY_ID, 'new_key')
        self.assertIn("OSS configuration for oss_access_key_id has been updated.", log.output[0])

    def test_update_alluxio_config(self):
        manager = ConfigManager()
        with self.assertLogs(level='INFO') as log:
            update_alluxio_config(Constants.ALLUXIO_FILESYSTEM_TYPE, ALLUXIO_ETCD_HOST, 'new_host')
        self.assertIn("Alluxio configuration for alluxio_etcd_host has been updated.", log.output[0])

    def test_update_config_unsupported_fs(self):
        manager = ConfigManager()
        with self.assertRaises(ValueError):
            manager.update_config('unsupported_fs_type', 'some_key', 'some_value')


if __name__ == '__main__':
    unittest.main()

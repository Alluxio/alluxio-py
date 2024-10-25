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

    def test_get_config_valid(self):
        manager = ConfigManager()
        oss_config = manager.get_config(Constants.OSS_FILESYSTEM_TYPE)
        self.assertEqual(oss_config, self.mock_config[Constants.OSS_FILESYSTEM_TYPE])

    def test_get_config_invalid(self):
        manager = ConfigManager()
        with self.assertRaises(ConfigMissingError):
            manager.get_config('nonexistent')


if __name__ == '__main__':
    unittest.main()

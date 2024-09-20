from alluxio.posix.const import Constants
from alluxio.posix.exception import ConfigMissingError

OSS_ACCESS_KEY_ID = 'access_key_id'
OSS_ACCESS_KEY_SECRET = 'access_key_secret'
OSS_ENDPOINT = 'endpoint'


def validate_oss_config(config):
    required_keys = [
        OSS_ACCESS_KEY_ID,
        OSS_ACCESS_KEY_SECRET,
        OSS_ENDPOINT,
        Constants.BUCKET_NAME
    ]

    for key in required_keys:
        if key not in config:
            raise ConfigMissingError(f"Missing required OSS config key: {key}")
        if not config[key]:
            raise ValueError(f"OSS config key '{key}' cannot be empty")


def update_oss_config(config_data, key, value):
    if key not in [
        OSS_ACCESS_KEY_ID,
        OSS_ACCESS_KEY_SECRET,
        OSS_ENDPOINT,
        Constants.BUCKET_NAME
    ]:
        raise ValueError(f"Invalid configuration key: {key}")
    config_data[key] = value
    validate_oss_config(config_data)
    return config_data

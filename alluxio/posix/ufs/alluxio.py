from alluxio.posix.exception import ConfigMissingError

ALLUXIO_ETCD_ENABLE = "alluxio_etcd_enable"
ALLUXIO_ETCD_HOST = 'alluxio_etcd_host'
ALLUXIO_WORKER_HOSTS = 'alluxio_worker_hosts'
ALLUXIO_BACKUP_FS = 'alluxio_backup_fs'
ALLUXIO_ENABLE = 'alluxio_enable'


def validate_alluxio_config(config):
    required_keys = []
    if config.get(ALLUXIO_ETCD_ENABLE, False):
        required_keys.append(ALLUXIO_ETCD_HOST)
    else:
        required_keys.append(ALLUXIO_WORKER_HOSTS)

    if not all(config.get(key) for key in required_keys):
        raise ConfigMissingError(f"The following keys must be set in the configuration: {required_keys}")


def update_alluxio_config(config_data, updates):
    allowed_keys = [
        'ALLUXIO_ETCD_ENABLE',
        'ALLUXIO_ETCD_HOST',
        'ALLUXIO_WORKER_HOSTS'
    ]
    for key, value in updates.items():
        if key not in allowed_keys:
            raise ValueError(f"Invalid configuration key for Alluxio: {key}")
        config_data[key] = value

    validate_alluxio_config(config_data)
    return config_data

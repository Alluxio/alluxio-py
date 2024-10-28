class ConfigMissingError(Exception):
    def __init__(self, config_key, message="Configuration key is missing"):
        self.config_key = config_key
        self.message = message
        super().__init__(f"{message}: {config_key}")


class ConfigInvalidError(Exception):
    def __init__(
        self, config_key, message="Configuration key is invalid, config_key"
    ):
        self.config_key = config_key
        self.message = message
        super().__init__(f"{message}: {config_key}")


class UnsupportedDelegateFileSystemError(Exception):
    def __init__(
        self, fs_name, message="FileSystem is not supported, filesystem"
    ):
        self.fs_name = fs_name
        self.message = message
        super().__init__(f"{message}: {fs_name}")

import configparser
import os
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Type
from typing import Union


class ConfigLoader:
    def __init__(self, config_path: str, interpolate: bool = True):
        self.config_path = os.path.expanduser(config_path)
        self._parser = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation()
            if interpolate
            else None
        )
        self.reload()

    def reload(self) -> None:
        read_files = self._parser.read(self.config_path, encoding="utf-8")
        if not read_files:
            raise FileNotFoundError(
                f"Config file not found: {self.config_path}"
            )

    def sections(self) -> List[str]:
        return self._parser.sections()

    def has_section(self, section: str) -> bool:
        return self._parser.has_section(section)

    def options(self, section: str) -> List[str]:
        return self._parser.options(section)

    def get(
        self,
        section: str,
        option: str,
        *,
        fallback: Optional[Any] = None,
        dtype: Type = str,
    ) -> Any:
        try:
            if dtype is bool:
                return self._parser.getboolean(
                    section, option, fallback=fallback
                )
            elif dtype is int:
                return self._parser.getint(section, option, fallback=fallback)
            elif dtype is float:
                return self._parser.getfloat(
                    section, option, fallback=fallback
                )
            else:
                val = self._parser.get(section, option, fallback=fallback)
                # 自动展开环境变量，例如 ${HOME}/data
                if isinstance(val, str):
                    return os.path.expandvars(val)
                return val
        except (
            configparser.NoSectionError,
            configparser.NoOptionError,
            ValueError,
        ):
            return fallback

    def as_dict(self) -> Dict[str, Dict[str, Union[str, int, float, bool]]]:
        result: Dict[str, Dict[str, Union[str, int, float, bool]]] = {}
        for sec in self.sections():
            opts = {}
            for key, raw_val in self._parser.items(sec):
                opts[key] = os.path.expandvars(raw_val)
            result[sec] = opts
        return result

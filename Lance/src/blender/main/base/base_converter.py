from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Dict

from Lance.src.blender.main.utils.handle_errors import handle_errors


class BaseConverter(ABC):
    @handle_errors("convert")
    def convert(self, source: str) -> Dict[str, Any]:
        return self._convert_impl(source)

    @abstractmethod
    def _convert_impl(self, source: str) -> Dict[str, Any]:
        ...

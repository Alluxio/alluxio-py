import os
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Optional

from Lance.src.blender.main.utils.handle_errors import handle_errors


class BaseDownloader(ABC):
    def __init__(self, name: str, cache_dir: Optional[str] = None):
        self.name = name
        self.cache_dir = cache_dir
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)

    @handle_errors("download")
    def download(self) -> Any:
        return self._download_impl()

    @abstractmethod
    def _download_impl(self) -> Any:
        ...

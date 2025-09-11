import os

from Lance.src.blender.main.base.base_converter import BaseConverter
from Lance.src.blender.main.converter.png_converter import PngConverter
from Lance.src.blender.main.converter.tensor_converter import TensorConverter


class ConverterFactory:
    _registry = {
        ".png": PngConverter,
        ".jpg": PngConverter,
        ".pt": TensorConverter,
        ".pth": TensorConverter,
    }

    @classmethod
    def get_converter(cls, filename: str) -> BaseConverter:
        ext = os.path.splitext(filename)[1].lower()
        converter_cls = cls._registry.get(ext)
        if not converter_cls:
            raise ValueError(f"No converter for extension: {ext}")
        return converter_cls()

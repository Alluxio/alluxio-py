import io
from typing import Any
from typing import Dict

from PIL import Image

from Lance.src.blender.main.base.base_converter import BaseConverter


class PngConverter(BaseConverter):
    def _convert_impl(self, source: str) -> Dict[str, Any]:
        with open(source, "rb") as f:
            img_bytes = f.read()

        img = Image.open(io.BytesIO(img_bytes))
        width, height = img.size
        channels = len(img.getbands())
        fmt = img.format.lower()

        return {
            "uri": source,
            "data": img_bytes,
            "height": height,
            "width": width,
            "channels": channels,
            "format": fmt,
        }

import io

import numpy as np
from PIL import Image

from Lance.src.blender.main.converter.png_converter import PngConverter


def create_dummy_png(path):
    # Generate a 2×2 small red image.
    img = Image.new("RGB", (2, 2), color=(255, 0, 0))
    img.save(path)


def test_png_converter_struct_and_values(tmp_path):
    png_path = tmp_path / "foo.png"
    create_dummy_png(png_path)

    conv = PngConverter()
    result = conv.convert(str(png_path))

    expected_keys = {"uri", "data", "height", "width", "channels", "format"}
    assert set(result.keys()) == expected_keys

    assert result["uri"].endswith("foo.png")
    assert result["height"] == 2
    assert result["width"] == 2
    assert result["channels"] == 3
    assert result["format"] == "png"
    assert isinstance(result["data"], (bytes, bytearray))
    assert len(result["data"]) > 0

    arr = np.array(Image.open(io.BytesIO(result["data"])))
    assert arr.shape == (2, 2, 3)
    assert np.all(arr[..., 0] == 255)
    assert np.all(arr[..., 1:] == 0)

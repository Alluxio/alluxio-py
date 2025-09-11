import io
import os
from typing import Any
from typing import Dict
from typing import Optional

import numpy as np
from PIL import Image


def to_feature_vector(row: Dict[str, Any], cols) -> np.ndarray:
    chunks = []
    for c in cols:
        v = row[c]
        arr = np.asarray(v).reshape(-1)
        chunks.append(arr)
    return np.concatenate(chunks, axis=0).astype("float32")


def decode_image(v: Any, image_root: Optional[str]) -> np.ndarray:
    if isinstance(v, dict) and "bytes" in v:
        im = Image.open(io.BytesIO(v["bytes"])).convert("RGB")
    elif isinstance(v, str):
        path = v
        if image_root and not os.path.isabs(path):
            path = os.path.join(image_root, path)
        im = Image.open(path).convert("RGB")
    else:
        raise ValueError(f"Unsupported image value type: {type(v)}")

    arr = np.array(im).astype("float32") / 255.0
    return arr  # HWC

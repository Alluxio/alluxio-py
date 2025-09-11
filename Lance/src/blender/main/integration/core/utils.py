import hashlib
from typing import Union


def stable_label_id(val: Union[str, int, float]) -> Union[int, float]:
    """
    Uniformly convert the labels into trainable numerical values:
    - str -> Stable hash (not affected by PYTHONHASHSEED)
    - int/float -> Original numerical values
    """
    if isinstance(val, str):
        h = hashlib.md5(val.encode("utf-8")).hexdigest()
        return int(h, 16) % (2**31 - 1)
    if isinstance(val, (int,)):
        return int(val)
    return float(val)

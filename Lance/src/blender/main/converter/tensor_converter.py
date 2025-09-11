from typing import Any
from typing import Dict

import numpy as np
import torch

from Lance.src.blender.main.base.base_converter import BaseConverter


class TensorConverter(BaseConverter):
    def _first_tensor(self, obj: Any):
        """递归地在 obj 中找到第一个 torch.Tensor。优先 obj['model']。"""
        import torch

        if isinstance(obj, torch.Tensor):
            return obj

        if isinstance(obj, dict):
            # 1) 优先 model（OpenNMT 常见）
            if "model" in obj and isinstance(obj["model"], dict):
                t = self._first_tensor(obj["model"])
                if t is not None:
                    return t
            # 2) 其次遍历其他键
            for v in obj.values():
                t = self._first_tensor(v)
                if t is not None:
                    return t
            return None

        if isinstance(obj, (list, tuple)):
            for v in obj:
                t = self._first_tensor(v)
                if t is not None:
                    return t
            return None

        return None

    def _convert_impl(self, source: str) -> Dict[str, Any]:
        obj = torch.load(source, map_location="cpu")

        # 尝试直接是 Tensor
        if isinstance(obj, torch.Tensor):
            t = obj
        else:
            t = self._first_tensor(obj)

        if t is None:
            # 给出更可读的报错信息，便于定位
            raise ValueError(
                f"未在 {source} 的 checkpoint 中找到任何 torch.Tensor。"
                " 这是一个嵌套结构（如 {'model': state_dict, ...}）吗？"
            )

        # 转 numpy（统一为 float32，避免半精度/整型造成后续不一致）
        arr = t.detach().cpu().numpy()
        if arr.dtype != np.float32:
            arr = arr.astype(np.float32, copy=False)

        return {
            "uri": source,
            "tensor": arr,
        }

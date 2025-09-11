# integration/pytorch/transforms.py
from typing import Callable
from typing import Dict

import torch
import torchvision.transforms.functional as TF
from torchvision.transforms import InterpolationMode


def make_image_transform(
    size: int = 224, augment: bool = False
) -> Callable[[Dict[str, torch.Tensor]], Dict[str, torch.Tensor]]:
    """
    输入: {'pixel_values': (C,H,W) float32[0,1], ...}
    输出: 同键，但像素统一为 (C,size,size)
    """

    def _t(sample: Dict[str, torch.Tensor]) -> Dict[str, torch.Tensor]:
        if "pixel_values" in sample:
            x = sample["pixel_values"]  # C,H,W
            # torchvision F.resize 支持 torch.Tensor(H,W) 或 (C,H,W)（需先换到 HWC 再回 C H W）
            x_hwc = x.permute(1, 2, 0)  # H,W,C
            x_hwc = TF.resize(
                x_hwc,
                [size, size],
                interpolation=InterpolationMode.BICUBIC,
                antialias=True,
            )
            x = x_hwc.permute(2, 0, 1)  # C,H,W
            if augment:
                # 轻量增强示例：水平翻转
                if torch.rand(()) < 0.5:
                    x = torch.flip(x, dims=[2])  # 水平翻转
            sample["pixel_values"] = x
        return sample

    return _t

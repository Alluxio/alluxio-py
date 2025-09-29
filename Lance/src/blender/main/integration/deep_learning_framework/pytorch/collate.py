from typing import Dict
from typing import List

import torch
from torch.nn.utils.rnn import pad_sequence

_PAD_KEYS = (
    "input_ids",
    "attention_mask",
    "token_type_ids",
    "text_ids",
    "text_bytes",
)


def text_collate_fn(
    batch: List[Dict[str, torch.Tensor]]
) -> Dict[str, torch.Tensor]:
    out: Dict[str, torch.Tensor] = {}
    if not batch:
        return out

    keys = set().union(*[b.keys() for b in batch])

    for k in keys:
        if k in _PAD_KEYS and k in batch[0]:
            seqs = [b[k] for b in batch if k in b]
            # 统一 int 类型
            dtype = (
                torch.long
                if seqs[0].dtype in (torch.int32, torch.int64, torch.uint8)
                else seqs[0].dtype
            )
            seqs = [s.to(dtype) for s in seqs]
            out[k] = pad_sequence(seqs, batch_first=True, padding_value=0)

    for k in keys - set(_PAD_KEYS):
        if k not in batch[0]:
            continue
        vals = [b[k] for b in batch if k in b]
        if isinstance(vals[0], torch.Tensor):
            try:
                out[k] = torch.stack(vals, dim=0)
            except RuntimeError:
                out[k] = pad_sequence(vals, batch_first=True, padding_value=0)
        else:
            pass

    return out


def image_collate_fn(
    batch: List[Dict[str, torch.Tensor]]
) -> Dict[str, torch.Tensor]:
    out: Dict[str, torch.Tensor] = {}
    if not batch:
        return out
    keys = set().union(*[b.keys() for b in batch])

    # 图片像素：要求上游 transform 已保证相同尺寸 (C,H,W)
    if "pixel_values" in keys:
        imgs = [b["pixel_values"] for b in batch if "pixel_values" in b]
        out["pixel_values"] = torch.stack(imgs, dim=0)  # (B,C,H,W)

    # 其他键（labels/features/文本 token 等）
    for k in keys - {"pixel_values"}:
        if k not in batch[0]:
            continue
        vals = [b[k] for b in batch if k in b]
        if isinstance(vals[0], torch.Tensor):
            try:
                out[k] = torch.stack(vals, dim=0)
            except RuntimeError:
                out[k] = pad_sequence(vals, batch_first=True, padding_value=0)
    return out

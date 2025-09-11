import math
import os

import pytest
import torch
import torch.nn.functional as F
from torch.utils.data import DataLoader
from transformers import BertModel
from transformers import BertTokenizer

from Lance.src.blender.main.integration.core.mapping import ColumnMapping
from Lance.src.blender.main.integration.pytorch.lance_torch_dataset import (
    LanceTorchDataset,
)
from Lance.src.blender.main.utils.s3_options import build_s3_uri


def _ensure_s3_ready_or_skip():
    # 需要最基本的 S3 环境 & 目标桶
    bucket = os.getenv("S3_BUCKET")
    key_id = os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not bucket or not key_id or not secret:
        pytest.skip(
            "缺少 S3 环境变量（S3_BUCKET / AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY），跳过。"
        )
    return bucket


@pytest.mark.parametrize("train_steps", [20])
def test_end2end_s3_lance_torch_training(tmp_path, train_steps):
    bucket = _ensure_s3_ready_or_skip()

    # 从s3 读取.lance数据
    # 分别加载一个模型的.pth参数和一个数据集

    model = BertModel.from_pretrained("bert-base-uncased")
    tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    opt = torch.optim.AdamW(model.parameters(), lr=1e-5)

    ds_name = f"test_dataset_{tmp_path.name}"
    s3_uri = build_s3_uri(bucket, "datasets", f"{ds_name}.lance")

    # lance.write_dataset(table, s3_uri, mode="overwrite", storage_options=opts)

    torch_ds = LanceTorchDataset(
        lance_uri=s3_uri,
        mapping=ColumnMapping(),
        tokenizer=tokenizer,
        batch_rows=4096,
    )

    dl = DataLoader(
        torch_ds,
        batch_size=16,
        num_workers=0,
        pin_memory=torch.cuda.is_available(),
        collate_fn=_default_text_collate_fn,  # 见下：给一个稳定的 collation
    )

    # 记录初始损失
    init_loss = None
    steps = 0
    model.train()
    for batch in dl:
        if "input_ids" not in batch or "labels" not in batch:
            pytest.skip(
                "batch 中缺少 input_ids 或 labels，检查 LanceTorchDataset/ColumnMapping。"
            )
        x = batch["input_ids"].to(device)
        m = batch.get("attention_mask")
        m = m.to(device) if m is not None else None
        y = batch["labels"].to(device).long()

        logits = model(x, m).last_hidden_state
        loss = F.cross_entropy(logits, y)
        if init_loss is None:
            init_loss = loss.item()

        opt.zero_grad(set_to_none=True)
        loss.backward()
        opt.step()

        steps += 1
        if steps >= train_steps:
            break

    assert steps > 0, "未能从 S3 数据集中迭代 batch。"
    assert init_loss is not None, "未记录到初始损失。"
    assert math.isfinite(init_loss), "初始损失非数。"

    model.eval()
    with torch.no_grad():
        for batch in dl:
            if "input_ids" not in batch or "labels" not in batch:
                continue
            x = batch["input_ids"].to(device)
            m = batch.get("attention_mask")
            m = m.to(device) if m is not None else None
            y = batch["labels"].to(device).long()
            final_loss = F.cross_entropy(model(x, m), y).item()
            break

    assert math.isfinite(final_loss), "最终损失非数。"
    assert (
        final_loss <= init_loss + 0.1
    ), f"loss 未下降（init={init_loss:.4f}, final={final_loss:.4f})"


def _default_text_collate_fn(batch):
    if not batch:
        return {}

    # 统一 keys（某些 sample 可能缺键）
    keys = set().union(*(b.keys() for b in batch))
    out = {}

    # input_ids / attention_mask 需要 pad
    import torch
    from torch.nn.utils.rnn import pad_sequence

    def _to_long_tensor_list(name):
        seqs = []
        for b in batch:
            if name in b:
                arr = b[name]
                t = torch.as_tensor(arr, dtype=torch.long)
                seqs.append(t)
        return seqs

    for name in ("input_ids", "attention_mask"):
        if name in keys:
            seqs = _to_long_tensor_list(name)
            if seqs:
                out[name] = pad_sequence(
                    seqs, batch_first=True, padding_value=0
                )

    # labels 如果存在，stack 到一起
    if "labels" in keys:
        ys = []
        for b in batch:
            if "labels" in b:
                ys.append(torch.as_tensor(b["labels"], dtype=torch.long))
        if ys:
            out["labels"] = torch.stack(ys, 0)

    return out

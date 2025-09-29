import argparse
import os
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List

import fsspec
import torch
from torch import nn
from torch.utils.data import DataLoader
from torchvision.models import resnet18
from tqdm import tqdm

from Lance.src.blender.main.converter.png_converter import PngConverter
from Lance.src.blender.main.converter.tensor_converter import TensorConverter
from Lance.src.blender.main.dataset.pytorch.LanceImageDataset import (
    LanceImageDataset,
)
from Lance.src.blender.main.downloader.dataset_downloader import (
    DatasetDownloader,
)
from Lance.src.blender.main.downloader.model_downloader import ModelDownloader
from Lance.src.blender.main.utils.s3_options import _mk_s3fs_kwargs


def _resolve_ckpt_path(
    mres: Dict[str, Any], cache_dir: str, model_name: str
) -> str:
    """Return a local checkpoint file path from downloader result."""
    kind = mres.get("kind")
    if kind == "url":
        p = mres["path"]
        if not os.path.exists(p):
            raise FileNotFoundError(f"url path missing: {p}")
        return p
    if kind == "transformers":
        tmp = os.path.join(
            cache_dir, f"{Path(model_name).name or 'model'}_state.pth"
        )
        torch.save(mres["model"].state_dict(), tmp)
        return tmp
    # repo / local dir: pick largest ckpt-looking file
    root = Path(mres["path"])
    exts = [".pth", ".pt", ".bin", ".safetensors"]
    cands: List[Path] = []
    for ext in exts:
        cands.extend(root.rglob(f"*{ext}"))
    if not cands:
        listed = [
            str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()
        ]
        raise FileNotFoundError(
            f"no checkpoint under {root}; looked for {exts}; found: {listed[:50]}"
        )
    cands.sort(key=lambda p: p.stat().st_size, reverse=True)
    return str(cands[0])


def _model_tag_from_path(p: str) -> str:
    b = os.path.basename(p.split("?")[0])
    n, _ = os.path.splitext(b)
    return n or "model"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default="./_cache")
    ap.add_argument(
        "--model",
        default="https://download.pytorch.org/models/resnet18-f37072fd.pth",
    )
    ap.add_argument(
        "--model_mode", default="url", choices=["url", "repo", "transformers"]
    )
    ap.add_argument("--dataset", default="cifar10")
    ap.add_argument(
        "--dataset_mode", default="arrow", choices=["repo", "arrow"]
    )
    ap.add_argument("--split", default="train")
    ap.add_argument("--limit", type=int, default=1000)
    ap.add_argument("--batch", type=int, default=32)
    ap.add_argument("--epochs", type=int, default=1)
    ap.add_argument("--steps", type=int, default=50)
    ap.add_argument("--s3-endpoint", default="http://localhost:9000")
    ap.add_argument("--aws-access-key", default="minioadmin")
    ap.add_argument("--aws-secret-key", default="minioadmin")
    ap.add_argument("--aws-region", default="us-east-1")
    ap.add_argument("--bucket", default="my-bucket")
    ap.add_argument("--prefix", default="resnet_demo")
    args = ap.parse_args()

    # Prepare storage options for Lance + fsspec
    os.makedirs(args.cache, exist_ok=True)
    so = {
        "aws_access_key_id": str(args.aws_access_key),
        "aws_secret_access_key": str(args.aws_secret_key),
        "region": str(args.aws_region or "us-east-1"),
        "endpoint": str(args.s3_endpoint),
    }
    so_lance = {
        "aws_access_key_id": so["aws_access_key_id"],
        "aws_secret_access_key": so["aws_secret_access_key"],
        "region": so["region"],
        "endpoint": so["endpoint"],  # endpoint
        "allow_http": "true",  # Lance-specific toggle as string
        "force_path_style": "true",
        "virtual_hosted_style": "false",
    }

    # -------- 1) download & write weights to Lance (use TensorConverter) --------
    md = ModelDownloader(
        name=args.model, cache_dir=args.cache, mode=args.model_mode
    )
    mres = md.download()
    ckpt_path = _resolve_ckpt_path(
        mres, cache_dir=args.cache, model_name=args.model
    )

    weights_uri = f"s3://{args.bucket}/{args.prefix}/weights/{_model_tag_from_path(ckpt_path)}.lance"
    tensorConverter = TensorConverter()
    out = tensorConverter.convert(ckpt_path)  # out["table"] 是 Arrow 表
    tensorConverter.write_lance(
        out["table"],
        weights_uri,
        mode="overwrite",
        storage_options=so_lance,
    )
    pngConverter = PngConverter()
    # -------- 2) download dataset & write images to Lance (use PngConverter) --------
    dd = DatasetDownloader(
        name=args.dataset,
        cache_dir=args.cache,
        mode=args.dataset_mode,
        split=args.split,
    )
    d = dd.download()
    split_dir = d.get(args.split) if isinstance(d, dict) else d

    if args.dataset_mode == "arrow":
        img_tbl = pngConverter.images_to_lance_from_arrow(
            split_dir, limit=args.limit
        )
    else:
        # Using our folder scanner that validates real images and builds a single unified table
        img_tbl = pngConverter.folder_to_table(
            split_dir,
            limit=args.limit,
            class_from_parent=True,
            store_uri_relative=True,
        )

    images_uri = f"s3://{args.bucket}/{args.prefix}/datasets/{args.dataset}-{args.split}.lance"
    pngConverter.write_lance(
        img_tbl, images_uri, mode="overwrite", storage_options=so_lance
    )

    # -------- 3) build PyTorch loader --------
    train_ds = LanceImageDataset(images_uri, storage_options=so_lance)
    num_classes = len(set(train_ds.labels))
    loader = DataLoader(
        train_ds,
        batch_size=args.batch,
        shuffle=True,
        num_workers=2,
        pin_memory=torch.cuda.is_available(),
    )

    # -------- 4) init model & load backbone weights from Lance --------
    model = resnet18(weights=None)
    model.fc = nn.Linear(model.fc.in_features, num_classes)

    # NOTE: use strip_prefixes instead of strip_module_prefix (aligned with converter API)
    res = TensorConverter.load_from_lance_into_model(
        weights_uri,
        model,
        storage_options=so_lance,
        strip_prefixes=("module.",),  # strip "module." from DDP checkpoints
        ignore_prefixes=("fc.",),  # skip classifier head
        enforce_backbone_shape=True,  # drop shape-mismatch safely
    )

    def _all_fc(keys):
        return all(str(k).startswith("fc.") for k in keys)

    if not (
        _all_fc(res["missing_keys"])
        and _all_fc(res["unexpected_keys"])
        and all(k.startswith("fc.") for k in res["skipped_by_name"])
        and all(k.startswith("fc.") for k, _, __ in res["skipped_by_shape"])
    ):
        raise RuntimeError(
            "Backbone alignment failed.\n"
            f"missing_keys={res['missing_keys']}\n"
            f"unexpected_keys={res['unexpected_keys']}\n"
            f"skipped_by_name={res['skipped_by_name']}\n"
            f"skipped_by_shape={res['skipped_by_shape']}\n"
            "Expected only fc.* to be skipped/mismatched."
        )

    print(
        f"[OK] Loaded {len(res['loaded_keys'])} backbone params. "
        f"Skipped fc.*: {set(res['skipped_by_name']) | set(k for k, _, __ in res['skipped_by_shape'])}"
    )

    # -------- 5) train --------
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device).train()
    opt = torch.optim.SGD(model.parameters(), lr=0.01, momentum=0.9)
    loss_fn = nn.CrossEntropyLoss()

    for ep in range(args.epochs):
        model.train()
        n = 0
        pbar = tqdm(
            loader,
            total=min(args.steps, len(loader)),
            desc=f"epoch {ep + 1}/{args.epochs}",
        )
        for x, y in pbar:
            x, y = x.to(device, non_blocking=True), y.to(
                device, non_blocking=True
            )
            opt.zero_grad(set_to_none=True)
            out = model(x)
            loss = loss_fn(out, y)
            loss.backward()
            opt.step()
            n += 1
            pbar.set_postfix(loss=float(loss.detach().cpu()))
            if n >= args.steps:
                break

    # -------- 6) save trained weights to S3 --------
    out_local = os.path.join(args.cache, "resnet18_trained.pth")
    torch.save(model.state_dict(), out_local)

    s3_path = f"s3://{args.bucket}/{args.prefix}/models/resnet18_trained.pth"
    fs = fsspec.filesystem(
        "s3",
        **_mk_s3fs_kwargs(
            {
                "key": so["aws_access_key_id"],
                "secret": so["aws_secret_access_key"],
                "client_kwargs": {
                    "endpoint_url": so["endpoint"],
                    "region_name": so["region"],
                },
            }
        ),
    )
    with open(out_local, "rb") as fsrc, fs.open(s3_path, "wb") as fdst:
        fdst.write(fsrc.read())


if __name__ == "__main__":
    main()

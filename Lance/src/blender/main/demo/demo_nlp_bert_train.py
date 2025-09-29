import argparse
import json
import os
import shutil
from typing import Any
from typing import Dict

import fsspec
import lance
import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import torch
from torch.optim import AdamW
from torch.utils.data import DataLoader
from transformers import AutoConfig
from transformers import AutoModelForSequenceClassification
from transformers import AutoTokenizer
from transformers import get_linear_schedule_with_warmup

from Lance.src.blender.main.converter.language_converter import TextConverter
from Lance.src.blender.main.converter.tensor_converter import TensorConverter
from Lance.src.blender.main.integration.core.mapping import ColumnMapping
from Lance.src.blender.main.integration.deep_learning_framework.pytorch.lance_torch_dataset import (
    LanceTorchDataset,
)
from Lance.src.blender.main.utils.s3_options import get_s3_storage_options


def _mk_s3fs_kwargs(storage_options: Dict[str, Any]) -> Dict[str, Any]:
    key = storage_options.get("aws_access_key_id") or storage_options.get(
        "key"
    )
    secret = storage_options.get(
        "aws_secret_access_key"
    ) or storage_options.get("secret")
    region = storage_options.get("region") or storage_options.get(
        "client_kwargs", {}
    ).get("region_name")
    endpoint = storage_options.get("endpoint_override") or storage_options.get(
        "client_kwargs", {}
    ).get("endpoint_url")
    kwargs: Dict[str, Any] = {}
    if key:
        kwargs["key"] = key
    if secret:
        kwargs["secret"] = secret
    client_kwargs = {}
    if endpoint:
        client_kwargs["endpoint_url"] = endpoint
    if region:
        client_kwargs["region_name"] = region
    if client_kwargs:
        kwargs["client_kwargs"] = client_kwargs
    return kwargs


def open_with_fsspec(path: str, mode: str, storage_options: Dict[str, Any]):
    if path.startswith("s3://"):
        fs = fsspec.filesystem("s3", **_mk_s3fs_kwargs(storage_options))
        return fs.open(path, mode)
    return fsspec.open(path, mode)


def copy_to_s3(src: str, dst: str, storage_options: Dict[str, Any]):
    with fsspec.open(src, "rb") as fi, open_with_fsspec(
        dst, "wb", storage_options
    ) as fo:
        shutil.copyfileobj(fi, fo)
    return dst


def _infer_format_from_path(path: str) -> str:
    p = path.lower()
    if p.endswith(".csv"):
        return "csv"
    if p.endswith(".json") or p.endswith(".jsonl"):
        return "jsonl"
    if p.endswith(".parquet") or p.endswith(".pq"):
        return "parquet"
    if p.endswith(".lance"):
        return "lance"
    raise ValueError(path)


def load_table_from_s3(input_uri: str, storage_options: Dict[str, Any]):
    fmt = _infer_format_from_path(input_uri)
    if fmt == "csv":
        with fsspec.open(input_uri, "rb") as f:
            return pacsv.read_csv(f)
    if fmt == "jsonl":
        with fsspec.open(input_uri, "rt") as f:
            rows = [json.loads(line) for line in f]
        return pa.Table.from_pylist(rows)
    if fmt == "parquet":
        with fsspec.open(input_uri, "rb") as f:
            return pq.read_table(f)
    if fmt == "lance":
        return lance.dataset(input_uri)
    raise ValueError(input_uri)


def write_lance_to_s3(
    table_or_ds, lance_uri: str, storage_options: Dict[str, Any]
):
    if isinstance(table_or_ds, pa.Table):
        lance.write_dataset(
            table_or_ds, lance_uri, storage_options=storage_options
        )
        return
    raise TypeError()


def numpy_to_lance_table(arr: np.ndarray, name: str = "tensor") -> pa.Table:
    if arr.dtype != np.float32:
        arr = arr.astype(np.float32, copy=False)
    flat = arr.reshape(-1)
    col = pa.array(flat.tolist(), type=pa.float32())
    tbl = pa.table({name: col})
    meta = dict(tbl.schema.metadata or {})
    meta[b"shape"] = json.dumps(list(arr.shape)).encode("utf8")
    return tbl.replace_schema_metadata(meta)


class TextLabelMapper:
    def __init__(self):
        self.label2id: Dict[str, int] = {}
        self.id2label: Dict[int, str] = {}

    def fit(self, labels):
        uniq = sorted({str(x) for x in labels})
        self.label2id = {l: i for i, l in enumerate(uniq)}
        self.id2label = {i: l for l, i in self.label2id.items()}
        return self

    def transform(self, labels):
        return [self.label2id[str(x)] for x in labels]


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--s3-endpoint", type=str, default=os.getenv("S3_ENDPOINT", "")
    )
    p.add_argument("--s3-bucket", type=str, required=True)
    p.add_argument(
        "--aws-region", type=str, default=os.getenv("AWS_REGION", "us-east-1")
    )
    p.add_argument(
        "--aws-access-key",
        type=str,
        default=os.getenv("AWS_ACCESS_KEY_ID", ""),
    )
    p.add_argument(
        "--aws-secret-key",
        type=str,
        default=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    )
    p.add_argument("--raw-src", type=str, default="")
    p.add_argument("--raw-dst", type=str, default="")
    p.add_argument("--dataset-input", type=str, required=True)
    p.add_argument("--dataset-name", type=str, required=True)
    p.add_argument("--text-col", type=str, default="text")
    p.add_argument("--label-col", type=str, default="label")
    p.add_argument("--model-id", type=str, default="bert-base-uncased")
    p.add_argument("--out-prefix", type=str, required=True)
    p.add_argument("--batch-size", type=int, default=16)
    p.add_argument("--epochs", type=int, default=1)
    p.add_argument("--lr", type=float, default=5e-5)
    p.add_argument("--max-length", type=int, default=256)
    p.add_argument("--fp16", action="store_true")
    p.add_argument("--ckpt-input", type=str, default="")
    p.add_argument("--weights-out", type=str, default="")
    p.add_argument("--use-text-converter", action="store_true")
    args = p.parse_args()

    storage_options = get_s3_storage_options(
        override={
            "region": args.aws_region,
            "aws_access_key_id": args.aws_access_key or None,
            "aws_secret_access_key": args.aws_secret_key or None,
            "endpoint_override": args.s3_endpoint or None,
        }
    )

    if args.raw_src and args.raw_dst:
        print(f"[0/5] {args.raw_src} -> {args.raw_dst}")
        copy_to_s3(args.raw_src, args.raw_dst, storage_options)

    print("[1/5] load raw:", args.dataset_input)
    if args.use_text_converter and TextConverter is not None:
        tmp_local = os.path.abspath(
            "./_tmp_raw" + os.path.splitext(args.dataset_input)[1]
        )
        with fsspec.open(args.dataset_input, "rb") as fi, open(
            tmp_local, "wb"
        ) as fo:
            shutil.copyfileobj(fi, fo)
        table = TextConverter().convert(tmp_local)["table"]
    else:
        table = load_table_from_s3(args.dataset_input, storage_options)

    names = set(table.column_names)
    if args.text_col not in names or args.label_col not in names:
        raise KeyError(str(table.column_names))

    label_arr = table[args.label_col].to_pylist()
    mapper = TextLabelMapper().fit(label_arr)
    if not all(isinstance(x, int) for x in label_arr):
        new_labels = pa.array(mapper.transform(label_arr), type=pa.int64())
        table = table.set_column(
            table.schema.get_field_index(args.label_col),
            args.label_col,
            new_labels,
        )
        meta = dict(table.schema.metadata or {})
        meta[b"label2id"] = json.dumps(mapper.label2id).encode("utf8")
        table = table.replace_schema_metadata(meta)

    lance_uri = (
        f"{args.out_prefix.rstrip('/')}/datasets/{args.dataset_name}.lance"
    )
    print("[1/5] write lance:", lance_uri)
    write_lance_to_s3(table, lance_uri, storage_options)

    if args.ckpt_input and args.weights_out and TensorConverter is not None:
        print(f"[1.a] ckpt -> lance: {args.ckpt_input} -> {args.weights_out}")
        ckpt_tmp = os.path.abspath("./_tmp_ckpt.pth")
        with fsspec.open(args.ckpt_input, "rb") as fi, open(
            ckpt_tmp, "wb"
        ) as fo:
            shutil.copyfileobj(fi, fo)
        tout = TensorConverter().convert(ckpt_tmp)
        ttbl = numpy_to_lance_table(tout["tensor"], name="tensor")
        write_lance_to_s3(ttbl, args.weights_out, storage_options)

    print("[1.5/5] read-back lance")
    ds = lance.dataset(lance_uri, storage_options=storage_options)
    print("schema:", ds.schema)
    print("sample:", ds.to_table(limit=3).to_pylist())

    print("[2/5] tokenizer+model")
    tokenizer = AutoTokenizer.from_pretrained(args.model_id)
    num_labels = len(set(ds.to_table(columns=[args.label_col]).to_pylist()))
    config = AutoConfig.from_pretrained(args.model_id, num_labels=num_labels)
    model = AutoModelForSequenceClassification.from_pretrained(
        args.model_id, config=config
    )

    print("[3/5] dataset+dataloader")

    mapping = ColumnMapping()
    mapping.text = args.text_col
    mapping.label = args.label_col
    train_ds = LanceTorchDataset(
        lance_uri=lance_uri,
        mapping=mapping,
        tokenizer=tokenizer,
        batch_rows=8192,
        filter=None,
        columns=None,
    )

    train_loader = DataLoader(
        train_ds, batch_size=args.batch_size, shuffle=True
    )

    print("[4/5] train")
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)
    total_steps = len(train_loader) * args.epochs
    optimizer = AdamW(model.parameters(), lr=args.lr, weight_decay=0.01)
    scheduler = get_linear_schedule_with_warmup(optimizer, 0, total_steps)
    scaler = torch.cuda.amp.GradScaler(enabled=args.fp16)
    model.train()
    for _ in range(args.epochs):
        for batch in train_loader:
            batch = {k: v.to(device) for k, v in batch.items()}
            with torch.cuda.amp.autocast(enabled=args.fp16):
                out = model(**batch)
                loss = out.loss
            scaler.scale(loss).backward()
            scaler.step(optimizer)
            scaler.update()
            optimizer.zero_grad(set_to_none=True)
            scheduler.step()

    save_dir = f"{args.out_prefix.rstrip('/')}/models/{args.dataset_name}-{args.model_id.replace('/', '_')}"
    print("[5/5] save:", save_dir)
    model.save_pretrained(save_dir)
    tokenizer.save_pretrained(save_dir)
    print("done")


if __name__ == "__main__":
    main()

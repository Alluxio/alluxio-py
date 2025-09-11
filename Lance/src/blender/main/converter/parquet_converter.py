import glob
import gzip
import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any
from typing import Dict

import pyarrow.dataset as ds
import pyarrow.parquet as pq

from Lance.src.blender.main.base.base_converter import BaseConverter


class ParquetConverter(BaseConverter):
    def __init__(
        self, *, batch_rows: int = 128_000, eager_max_rows: int = 2_000_000
    ):
        self.batch_rows = batch_rows
        self.eager_max_rows = eager_max_rows

    def _convert_impl(self, source: str) -> Dict[str, Any]:
        p = Path(source)

        if p.suffix.lower() == ".zip":
            with tempfile.TemporaryDirectory() as td:
                with zipfile.ZipFile(source) as zf:
                    zf.extractall(td)
                candidates = list(Path(td).rglob("*.parquet"))
                if not candidates:
                    raise ValueError(
                        "The .parquet file was not found in the ZIP file."
                    )
                dataset = ds.dataset(
                    [str(c) for c in candidates],
                    format="parquet",
                    partitioning="hive",
                )
                scanner = dataset.scanner()
                reader = scanner.to_reader(self.batch_rows)
                return {
                    "uri": source,
                    "reader": reader,
                    "schema": dataset.schema,
                }

        if any(ch in source for ch in "*?[]"):
            files = glob.glob(source)
            if not files:
                raise FileNotFoundError(f"No file matched: {source}")
            dataset = ds.dataset(files, format="parquet", partitioning="hive")
            scanner = dataset.scanner()
            reader = scanner.to_reader(self.batch_rows)
            return {"uri": source, "reader": reader, "schema": dataset.schema}

        if p.is_dir():
            dataset = ds.dataset(str(p), format="parquet", partitioning="hive")
            scanner = dataset.scanner()
            reader = scanner.to_reader(self.batch_rows)
            return {"uri": source, "reader": reader, "schema": dataset.schema}

        suffix = p.suffix.lower()
        if suffix == ".gz" and p.name.lower().endswith(".parquet.gz"):
            with tempfile.NamedTemporaryFile(
                suffix=".parquet", delete=False
            ) as tmp:
                with gzip.open(source, "rb") as fin:
                    shutil.copyfileobj(fin, tmp)
                tmp_path = tmp.name
            try:
                table = pq.read_table(tmp_path)
            finally:
                os.unlink(tmp_path)
            return {"uri": source, "table": table}

        if suffix in (".parquet", ".parq"):
            table = pq.read_table(source)
            if table.num_rows > self.eager_max_rows:
                dataset = ds.dataset([source], format="parquet")
                reader = dataset.scanner().to_reader(self.batch_rows)
                return {
                    "uri": source,
                    "reader": reader,
                    "schema": dataset.schema,
                }
            return {"uri": source, "table": table}

        raise ValueError(f"不支持的扩展名: {suffix}")

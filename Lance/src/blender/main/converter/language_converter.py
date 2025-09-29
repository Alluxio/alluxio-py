import json
import os
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import Tuple

import pandas as pd
import pyarrow as pa

from Lance.src.blender.main.base.base_converter import BaseConverter
from Lance.src.blender.main.utils.hash import row_sha256
from Lance.src.blender.main.utils.hash import sha256_stream


class TextConverter(BaseConverter):
    def __init__(self, natural_pk: Optional[str] = None):
        """
        :param natural_pk: optional column name to use as primary key if it exists
                           and is valid (non-null, unique). Otherwise fallback to row sha256.
        """
        self.natural_pk = natural_pk

    # ---------------- single file ----------------
    def _convert_impl(self, source: str) -> Dict[str, Any]:
        ext = os.path.splitext(source)[1].lower()

        # Load into a DataFrame
        if ext == ".csv":
            df = pd.read_csv(source)
        elif ext in (".json", ".ndjson"):
            try:
                df = pd.read_json(source, lines=True)
            except ValueError:
                with open(source, "r", encoding="utf-8") as f:
                    data = json.load(f)
                df = pd.json_normalize(data)
        elif ext in (".xls", ".xlsx"):
            sheets = pd.read_excel(source, sheet_name=None)
            df = pd.concat(sheets.values(), ignore_index=True)
        else:
            raise ValueError(f"Unsupported file extension: {ext}")

        df = df.reset_index(drop=True)

        # --- compute primary key BEFORE adding provenance columns ---
        pk_name = "_pk"
        if self.natural_pk and self.natural_pk in df.columns:
            series = df[self.natural_pk].astype(str)
            if series.isna().any() or series.duplicated().any():
                # fallback to row sha256 if natural_pk is not valid
                row_hashes = [
                    row_sha256(r.to_dict()) for _, r in df.iterrows()
                ]
                df["_pk"] = row_hashes
            else:
                df["_pk"] = series
                pk_name = self.natural_pk
        else:
            # row-level stable hash on original payload columns
            row_hashes = [row_sha256(r.to_dict()) for _, r in df.iterrows()]
            df["_pk"] = row_hashes

        # --- add provenance columns (not part of row hash) ---
        file_sha = sha256_stream(source)
        df["_source_uri"] = source
        df["_file_sha256"] = file_sha
        df["_row_index"] = df.index.astype("int64")

        table = pa.Table.from_pandas(df, preserve_index=False)
        return {
            "uri": source,
            "table": table,
            "primary_key": pk_name,
        }

    # ---------------- folder scan ----------------
    @staticmethod
    def _find_texts(
        root: str,
        suffixes: Tuple[str, ...] = (
            ".csv",
            ".json",
            ".ndjson",
            ".xls",
            ".xlsx",
        ),
        *,
        validate: bool = True,
        ignore_hidden: bool = True,
        exclude_dirs: Tuple[str, ...] = (
            ".git",
            ".svn",
            "__pycache__",
            ".idea",
        ),
        follow_symlinks: bool = False,
        limit: Optional[int] = None,
    ) -> List[Path]:
        """
        Recursively scan `root` and return a list of valid text-like files.
        - Filter by extension first.
        - Optionally do a quick validation (read a tiny sample) to avoid broken files.
        - Skip hidden files/dirs and common noise dirs by default.
        - Deterministic order (sorted).
        """

        def _quick_validate(path: Path) -> bool:
            ext = path.suffix.lower()
            try:
                if ext == ".csv":
                    pd.read_csv(path, nrows=8)  # small sample
                elif ext in (".json", ".ndjson"):
                    # Try line-delimited first; fall back to peeking head
                    try:
                        pd.read_json(path, lines=True, nrows=8)
                    except Exception:
                        with open(
                            path, "r", encoding="utf-8", errors="ignore"
                        ) as f:
                            head = f.read(2048).lstrip()
                            if not (
                                head.startswith("{") or head.startswith("[")
                            ):
                                return False
                elif ext in (".xls", ".xlsx"):
                    # Open metadata only; doesn't load all sheets to memory
                    pd.ExcelFile(path)
                else:
                    return False
                return True
            except Exception:
                return False

        out: List[Path] = []
        for dirpath, dirnames, filenames in os.walk(
            root, followlinks=follow_symlinks
        ):
            if ignore_hidden:
                dirnames[:] = [d for d in dirnames if not d.startswith(".")]
            if exclude_dirs:
                dirnames[:] = [d for d in dirnames if d not in exclude_dirs]

            for name in filenames:
                if ignore_hidden and name.startswith("."):
                    continue
                p = Path(dirpath) / name
                if p.suffix.lower() not in suffixes:
                    continue
                if validate and not _quick_validate(p):
                    continue
                out.append(p)
                if limit is not None and len(out) >= limit:
                    out.sort(key=lambda x: str(x).lower())
                    return out

        out.sort(key=lambda x: str(x).lower())
        return out

    @classmethod
    def folder_to_table(
        cls,
        dataset_dir: str,
        *,
        natural_pk: Optional[str] = None,
        file_limit: Optional[int] = None,
        validate: bool = True,
    ) -> pa.Table:
        """
        Convert all supported files under a folder into a single Arrow table.
        Merges different schemas by union of columns (missing -> NA).
        """
        files = cls._find_texts(
            dataset_dir, validate=validate, limit=file_limit
        )
        conv = cls(natural_pk=natural_pk)

        tables: List[pa.Table] = []
        for p in files:
            try:
                out = conv.convert(str(p))  # uses _convert_impl
                tables.append(out["table"])
            except Exception:
                # skip unreadable or malformed file
                continue

        if not tables:
            # empty table with no rows
            return pa.table({})

        # Best-effort concat; Arrow will promote types where possible
        return pa.concat_tables(tables, promote=True)

    # ---------------- write to Lance ----------------
    @staticmethod
    def write_lance(
        table: pa.Table,
        lance_uri: str,
        *,
        mode: Literal["overwrite", "append"] = "overwrite",
        storage_options: Optional[Dict[str, str]] = None,
        partition_cols: Optional[List[str]] = None,
    ) -> str:
        """Write the given Arrow table to a Lance dataset."""
        kw: Dict[str, Any] = {}
        if partition_cols:
            kw["partition_cols"] = partition_cols
        import lance

        lance.write_dataset(
            table,
            lance_uri,
            mode=mode,
            storage_options=storage_options or {},
            **kw,
        )
        return lance_uri

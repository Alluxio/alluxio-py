import json
import os
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import Tuple

import lance
import numpy as np
import pyarrow as pa
import torch

from Lance.src.blender.main.base.base_converter import BaseConverter
from Lance.src.blender.main.utils.hash import file_metadata
from Lance.src.blender.main.utils.hash import sha256_bytes
from Lance.src.blender.main.utils.hash import tensor_to_bytes


class TensorConverter(BaseConverter):
    """
    Minimal checkpoint(.pth/.pt/.ckpt/.bin/.safetensors) -> Lance converter.

    Row schema (per param):
      - _pk: string              # stable primary key
      - name: string             # param name
      - data: binary             # raw bytes of numpy array in 'C' order
      - dtype: string            # numpy dtype, e.g. 'float32'
      - shape: string            # JSON list of ints
      - tensor_sha256: string    # sha256 of 'data'
      - artifact_sha256: string  # file-level sha256 (streaming)
      - _source_uri: string      # original file path
      - model_id: string?        # optional tag
    """

    def __init__(
        self, model_id: Optional[str] = None, pk_mode: str = "artifact+name"
    ):
        """
        :param model_id: optional model identifier to attach to rows
        :param pk_mode: one of {"artifact+name", "artifact+tensor", "tensor"}.
                        - artifact+name:   _pk = f"{artifact_sha256}:{name}"
                        - artifact+tensor: _pk = f"{artifact_sha256}:{tensor_sha256}"
                        - tensor:          _pk = tensor_sha256
        """
        assert pk_mode in {"artifact+name", "artifact+tensor", "tensor"}
        self.model_id = model_id
        self.pk_mode = pk_mode

    # ---------------- single file ----------------
    def _load_any(self, source: str):
        """Load a checkpoint dict via safetensors or torch.load."""
        ext = Path(source).suffix.lower()
        if ext == ".safetensors":
            # lazy & safe; does not load all tensors into memory at once
            try:
                from safetensors.torch import load_file
            except Exception as e:
                raise ImportError("safetensors not installed") from e
            return load_file(source)
        # torch load fallback
        try:
            return torch.load(source, map_location="cpu", weights_only=True)
        except TypeError:
            # older PyTorch without weights_only
            return torch.load(source, map_location="cpu")

    @staticmethod
    def _extract_state_dict(obj: Any) -> Dict[str, torch.Tensor]:
        """Return a flat {name: Tensor} mapping."""
        if isinstance(obj, dict):
            # direct state_dict
            if obj and all(isinstance(v, torch.Tensor) for v in obj.values()):
                return obj
            # common wrappers
            for key in ("state_dict", "model", "weights"):
                inner = obj.get(key)
                if (
                    isinstance(inner, dict)
                    and inner
                    and all(
                        isinstance(v, torch.Tensor) for v in inner.values()
                    )
                ):
                    return inner
        raise ValueError(
            "Checkpoint does not contain a state_dict (name->tensor mapping)"
        )

    def _state_to_table(
        self,
        sd: Dict[str, torch.Tensor],
        source: str,
        artifact_sha256: str,
    ) -> pa.Table:
        """Convert state_dict into an Arrow table."""
        names: List[str] = []
        datas: List[bytes] = []
        dtypes: List[str] = []
        shapes: List[str] = []
        t_shas: List[str] = []
        a_shas: List[str] = []
        srcs: List[str] = []
        pks: List[str] = []
        model_ids: List[Optional[str]] = []

        for name, t in sd.items():
            if not isinstance(t, torch.Tensor):
                continue
            arr = t.detach().cpu().numpy()
            raw = tensor_to_bytes(arr, order="C")  # deterministic bytes
            ten_sha = sha256_bytes(raw)

            # choose primary key
            if self.pk_mode == "artifact+name":
                pk = f"{artifact_sha256}:{name}"
            elif self.pk_mode == "artifact+tensor":
                pk = f"{artifact_sha256}:{ten_sha}"
            else:
                pk = ten_sha

            names.append(name)
            datas.append(raw)
            dtypes.append(str(arr.dtype))
            shapes.append(json.dumps(list(arr.shape)))
            t_shas.append(ten_sha)
            a_shas.append(artifact_sha256)
            srcs.append(source)
            pks.append(pk)
            model_ids.append(self.model_id)

        return pa.table(
            {
                "_pk": pa.array(pks, type=pa.string()),
                "name": pa.array(names, type=pa.string()),
                "data": pa.array(datas, type=pa.binary()),
                "dtype": pa.array(dtypes, type=pa.string()),
                "shape": pa.array(shapes, type=pa.string()),
                "tensor_sha256": pa.array(t_shas, type=pa.string()),
                "artifact_sha256": pa.array(a_shas, type=pa.string()),
                "_source_uri": pa.array(srcs, type=pa.string()),
                "model_id": pa.array(
                    model_ids
                    if self.model_id is not None
                    else [None] * len(names),
                    type=pa.string(),
                ),
            }
        )

    def _convert_impl(self, source: str) -> Dict[str, Any]:
        """Convert one checkpoint file to a Lance-ready Arrow table (no write)."""
        obj = self._load_any(source)
        sd = self._extract_state_dict(obj)
        meta = file_metadata(source)  # streaming sha256
        art_sha = meta["artifact_sha256"]
        tbl = self._state_to_table(sd, source=source, artifact_sha256=art_sha)

        # Optional preview: first tensor as float32 numpy array
        first_name = next(iter(sd))
        preview = (
            sd[first_name]
            .detach()
            .cpu()
            .numpy()
            .astype(np.float32, copy=False)
        )

        return {
            "uri": source,
            "artifact_sha256": art_sha,
            "table": tbl,
            "tensor": preview,
            "primary_key": "_pk",
        }

    # ---------------- folder scan + merge ----------------
    @staticmethod
    def _find_checkpoints(
        root: str,
        suffixes: Tuple[str, ...] = (
            ".safetensors",
            ".pth",
            ".pt",
            ".ckpt",
            ".bin",
        ),
        *,
        validate: Literal["auto", "strict", False] = "auto",
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
        Recursively find checkpoint files. Lightweight validation by default:
        - .safetensors -> open metadata via safetensors (cheap)
        - others       -> file exists & > 0 bytes (strict-> torch.load)
        """

        def _is_valid(p: Path) -> bool:
            ext = p.suffix.lower()
            if validate is False:
                return True
            if ext == ".safetensors":
                if validate in ("auto", "strict"):
                    try:
                        from safetensors import safe_open

                        with safe_open(
                            str(p), framework="pt", device="cpu"
                        ) as f:
                            _ = list(f.keys())  # touch keys only
                        return True
                    except Exception:
                        return False
            else:
                # cheap check
                if p.stat().st_size <= 0:
                    return False
                if validate == "strict":
                    try:
                        # may be heavy; use only when you really want strict validation
                        _ = torch.load(str(p), map_location="cpu")
                        return True
                    except Exception:
                        return False
                return True
            return True

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
                if not _is_valid(p):
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
        model_id: Optional[str] = None,
        pk_mode: str = "artifact+name",
        file_limit: Optional[int] = None,
        validate: Literal["auto", "strict", False] = "auto",
    ) -> pa.Table:
        """
        Convert all checkpoints under a folder into ONE Arrow table (concat of all).
        """
        files = cls._find_checkpoints(
            dataset_dir, validate=validate, limit=file_limit
        )
        conv = cls(model_id=model_id, pk_mode=pk_mode)

        tables: List[pa.Table] = []
        for p in files:
            try:
                out = conv.convert(str(p))  # uses _convert_impl
                tables.append(out["table"])
            except Exception:
                # skip unreadable/malformed checkpoint
                continue

        return (
            pa.concat_tables(tables, promote=True) if tables else pa.table({})
        )

    # ---------------- write Lance ----------------
    @staticmethod
    def write_lance(
        table: pa.Table,
        lance_uri: str,
        *,
        mode: Literal["overwrite", "append"] = "overwrite",
        storage_options: Optional[Dict[str, str]] = None,
    ) -> str:
        """Write an Arrow table into a Lance dataset."""
        lance.write_dataset(
            table, lance_uri, mode=mode, storage_options=storage_options or {}
        )
        return lance_uri

    @classmethod
    def folder_to_lance(
        cls,
        dataset_dir: str,
        lance_uri: str,
        *,
        model_id: Optional[str] = None,
        pk_mode: str = "artifact+name",
        overwrite: bool = True,
        storage_options: Optional[Dict[str, str]] = None,
        file_limit: Optional[int] = None,
        validate: Literal["auto", "strict", False] = "auto",
    ) -> str:
        """Scan a folder, build a single table, and write to a Lance dataset."""
        tbl = cls.folder_to_table(
            dataset_dir,
            model_id=model_id,
            pk_mode=pk_mode,
            file_limit=file_limit,
            validate=validate,
        )
        mode = "overwrite" if overwrite else "append"
        return cls.write_lance(
            tbl, lance_uri, mode=mode, storage_options=storage_options
        )

    # ---------------- load back into a model ----------------
    @staticmethod
    def load_from_lance_into_model(
        lance_uri: str,
        model: torch.nn.Module,
        *,
        storage_options: Optional[Dict[str, str]] = None,
        strip_prefixes: Tuple[str, ...] = ("module.",),
        ignore_prefixes: Tuple[str, ...] = ("fc.",),
        enforce_backbone_shape: bool = True,
        allow_dtype_cast: bool = False,
        device: Optional[torch.device] = None,
    ) -> Dict[str, Any]:
        """
        Restore parameters from a Lance dataset back into a torch model (non-strict).
        This is the minimal loader you need for training.
        """
        ds = lance.dataset(lance_uri, storage_options=storage_options or {})
        tbl = ds.to_table()

        # rebuild state_dict(name->Tensor)
        names = tbl["name"].to_pylist()
        datas = tbl["data"].to_pylist()
        dtypes = tbl["dtype"].to_pylist()
        shapes = [json.loads(s) for s in tbl["shape"].to_pylist()]

        sd: Dict[str, torch.Tensor] = {}
        for n, b, dt, shp in zip(names, datas, dtypes, shapes):
            arr = np.frombuffer(b, dtype=np.dtype(dt)).copy().reshape(shp)
            sd[n] = torch.from_numpy(arr)

        # strip prefixes
        if strip_prefixes:
            from collections import OrderedDict

            fixed = OrderedDict()
            for k, v in sd.items():
                nk = k
                for pref in strip_prefixes:
                    if nk.startswith(pref):
                        nk = nk[len(pref) :]
                fixed[nk] = v
            sd = fixed

        model_sd = model.state_dict()
        filtered = {}
        skipped_by_name = []
        skipped_by_shape = []
        casted_dtype = []

        for k, v in sd.items():
            if any(k.startswith(pref) for pref in ignore_prefixes):
                skipped_by_name.append(k)
                continue
            if k not in model_sd:
                skipped_by_name.append(k)
                continue

            tgt = model_sd[k]
            if enforce_backbone_shape and tuple(tgt.shape) != tuple(v.shape):
                skipped_by_shape.append((k, tuple(v.shape), tuple(tgt.shape)))
                continue

            if allow_dtype_cast and v.dtype != tgt.dtype:
                v = v.to(dtype=tgt.dtype)
                casted_dtype.append(k)

            v = v.to(device=tgt.device if device is None else device)
            filtered[k] = v

        missing = model.load_state_dict(filtered, strict=False)
        return {
            "missing_keys": missing.missing_keys,
            "unexpected_keys": missing.unexpected_keys,
            "skipped_by_name": skipped_by_name,
            "skipped_by_shape": skipped_by_shape,
            "casted_dtype": casted_dtype,
            "loaded_keys": list(filtered.keys()),
        }

# audio_converter.py
from __future__ import annotations

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

from Lance.src.blender.main.base.base_converter import BaseConverter
from Lance.src.blender.main.utils.hash import file_metadata
from Lance.src.blender.main.utils.hash import sha256_bytes

_BACKEND = None
try:
    import librosa

    _BACKEND = "librosa"
except Exception:
    try:
        import soundfile as sf

        _BACKEND = "soundfile"
    except Exception:
        _BACKEND = None


class AudioConverter(BaseConverter):
    """
    Minimal audio-to-Lance converter.

    Row schema (per clip):
      - _pk: string              # stable primary key
      - audio: binary            # float32 mono PCM bytes
      - sample_rate: int32       # Hz (target_sr if resampled)
      - n_samples: int64         # number of samples in 'audio'
      - duration: float32        # seconds
      - channels: int32          # channels in *original* file
      - format: string           # original file extension (lowercased)
      - uri: string              # relative or absolute path
      - artifact_sha256: string  # file-level sha256 (streaming)
      - audio_sha256: string     # sha256 of PCM bytes
      - class: string            # optional class name from parent folder
      - label: int32             # numeric label (from class map), -1 if unknown
    """

    def __init__(
        self,
        target_sr: int = 16000,
        mono: bool = True,
        pk_mode: str = "artifact+audio",  # {"artifact", "audio", "artifact+audio"}
    ):
        """
        :param target_sr: resample to this rate if backend supports it (librosa).
        :param mono: convert to mono if True.
        :param pk_mode:
          - "artifact"        -> _pk = artifact_sha256
          - "audio"           -> _pk = audio_sha256
          - "artifact+audio"  -> _pk = f"{artifact_sha256}:{audio_sha256}" (default)
        """
        assert pk_mode in {"artifact", "audio", "artifact+audio"}
        self.target_sr = int(target_sr)
        self.mono = bool(mono)
        self.pk_mode = pk_mode

        if _BACKEND is None:
            raise ImportError(
                "No audio backend found. Install `librosa` or `soundfile`."
            )

    def _load_audio(self, source: str) -> Tuple[np.ndarray, int, int]:
        """
        Load audio as float32 mono PCM in [-1, 1].
        Returns (wave [N], sample_rate, orig_channels).
        """
        if _BACKEND == "librosa":
            import librosa as _librosa  # local import for static checkers

            # Best-effort get original channels via soundfile (optional)
            orig_ch = 1
            try:
                import soundfile as _sf

                orig_ch = int(_sf.info(source).channels)
            except Exception:
                pass

            # Always decode to mono at target_sr for a stable schema
            y, sr = _librosa.load(source, sr=self.target_sr, mono=True)
            y = np.asarray(y, dtype=np.float32, order="C")
            return y, int(sr), orig_ch

        elif _BACKEND == "soundfile":
            import soundfile as _sf  # local import

            # soundfile does not resample; keep original sr, convert to mono
            data, sr = _sf.read(
                source, always_2d=True, dtype="float32"
            )  # shape [N, C]
            orig_ch = int(data.shape[1])
            y = data.mean(axis=1).astype(np.float32, copy=False)  # mono
            return y, int(sr), orig_ch

        # Should not happen due to __init__ guard
        raise RuntimeError("No audio backend available at runtime.")

    def _row_from_file(
        self, source: str, *, cls: str = "", label: int = -1
    ) -> Dict[str, Any]:
        """Produce one row dict for a single audio file."""
        y, sr, orig_ch = self._load_audio(source)
        pcm = memoryview(y).tobytes(order="C")
        audio_sha = sha256_bytes(bytes(pcm))
        meta = file_metadata(source)  # streaming sha256
        art_sha = meta["artifact_sha256"]

        if self.pk_mode == "artifact":
            pk = art_sha
        elif self.pk_mode == "audio":
            pk = audio_sha
        else:
            pk = f"{art_sha}:{audio_sha}"

        return {
            "_pk": pk,
            "audio": pcm,
            "sample_rate": sr,
            "n_samples": len(y),
            "duration": float(len(y) / max(1, sr)),
            "channels": orig_ch,
            "format": Path(source).suffix.lower().lstrip("."),
            "uri": source,
            "artifact_sha256": art_sha,
            "audio_sha256": audio_sha,
            "class": cls,
            "label": int(label),
        }

    def _convert_impl(self, source: str) -> Dict[str, Any]:
        """Convert one file to a single-row Arrow table (no write)."""
        row = self._row_from_file(source)
        tbl = pa.table(
            {
                k: pa.array(
                    [row[k]],
                    type=(
                        pa.binary()
                        if k == "audio"
                        else pa.int32()
                        if k in ("sample_rate", "channels", "label")
                        else pa.int64()
                        if k == "n_samples"
                        else pa.float32()
                        if k == "duration"
                        else pa.string()
                    ),
                )
                for k in [
                    "_pk",
                    "audio",
                    "sample_rate",
                    "n_samples",
                    "duration",
                    "channels",
                    "format",
                    "uri",
                    "artifact_sha256",
                    "audio_sha256",
                    "class",
                    "label",
                ]
            }
        )
        return {"uri": source, "table": tbl, "primary_key": "_pk"}

    # ---------------- folder scan + table ----------------
    @staticmethod
    def _find_audios(
        root: str,
        suffixes: Tuple[str, ...] = (
            ".wav",
            ".flac",
            ".mp3",
            ".ogg",
            ".m4a",
            ".aac",
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
        Recursively find audio files and (optionally) do a cheap validation.
        Validation strategy:
          - librosa: use librosa.get_duration
          - soundfile fallback: sf.info
        """
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
                if validate:
                    try:
                        if _BACKEND == "librosa":
                            _ = librosa.get_duration(path=str(p))
                        else:
                            _ = sf.info(str(p))
                    except Exception:
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
        target_sr: int = 16000,
        mono: bool = True,
        pk_mode: str = "artifact+audio",
        file_limit: Optional[int] = None,
        validate: bool = True,
        class_from_parent: bool = True,
        store_uri_relative: bool = True,
    ) -> pa.Table:
        """
        Convert all supported audio files under a folder into ONE Arrow table.
        Class/label are inferred from parent folder name if class_from_parent=True.
        """
        files = cls._find_audios(
            dataset_dir, validate=validate, limit=file_limit
        )
        conv = cls(target_sr=target_sr, mono=mono, pk_mode=pk_mode)

        rows: List[Dict[str, Any]] = []
        base = Path(dataset_dir)
        label_map: Dict[str, int] = {}
        next_lbl = 0

        for p in files:
            cls_name = p.parent.name if class_from_parent else ""
            if cls_name not in label_map:
                label_map[cls_name] = next_lbl
                next_lbl += 1
            row = conv._row_from_file(
                str(p),
                cls=cls_name,
                label=label_map[cls_name] if class_from_parent else -1,
            )
            if store_uri_relative:
                row["uri"] = os.path.relpath(str(p), str(base))
            rows.append(row)

        if not rows:
            return pa.table({})

        # Build Arrow columns
        def arr(name, seq, typ):
            return pa.array(seq, type=typ)

        return pa.table(
            {
                "_pk": arr("_pk", [r["_pk"] for r in rows], pa.string()),
                "audio": arr("audio", [r["audio"] for r in rows], pa.binary()),
                "sample_rate": arr(
                    "sample_rate", [r["sample_rate"] for r in rows], pa.int32()
                ),
                "n_samples": arr(
                    "n_samples", [r["n_samples"] for r in rows], pa.int64()
                ),
                "duration": arr(
                    "duration", [r["duration"] for r in rows], pa.float32()
                ),
                "channels": arr(
                    "channels", [r["channels"] for r in rows], pa.int32()
                ),
                "format": arr(
                    "format", [r["format"] for r in rows], pa.string()
                ),
                "uri": arr("uri", [r["uri"] for r in rows], pa.string()),
                "artifact_sha256": arr(
                    "artifact_sha256",
                    [r["artifact_sha256"] for r in rows],
                    pa.string(),
                ),
                "audio_sha256": arr(
                    "audio_sha256",
                    [r["audio_sha256"] for r in rows],
                    pa.string(),
                ),
                "class": arr("class", [r["class"] for r in rows], pa.string()),
                "label": arr("label", [r["label"] for r in rows], pa.int32()),
            }
        )

    # ---------------- write Lance ----------------
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
        lance.write_dataset(
            table,
            lance_uri,
            mode=mode,
            storage_options=storage_options or {},
            **kw,
        )
        return lance_uri

    @classmethod
    def folder_to_lance(
        cls,
        dataset_dir: str,
        lance_uri: str,
        *,
        target_sr: int = 16000,
        mono: bool = True,
        pk_mode: str = "artifact+audio",
        overwrite: bool = True,
        storage_options: Optional[Dict[str, str]] = None,
        file_limit: Optional[int] = None,
        validate: bool = True,
        class_from_parent: bool = True,
        store_uri_relative: bool = True,
        partition_cols: Optional[List[str]] = None,
    ) -> str:
        """Scan a folder, build a single table, and write to a Lance dataset."""
        tbl = cls.folder_to_table(
            dataset_dir,
            target_sr=target_sr,
            mono=mono,
            pk_mode=pk_mode,
            file_limit=file_limit,
            validate=validate,
            class_from_parent=class_from_parent,
            store_uri_relative=store_uri_relative,
        )
        mode = "overwrite" if overwrite else "append"
        return cls.write_lance(
            tbl,
            lance_uri,
            mode=mode,
            storage_options=storage_options,
            partition_cols=partition_cols,
        )

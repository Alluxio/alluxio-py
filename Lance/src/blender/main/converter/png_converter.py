import io
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
from PIL import Image
from PIL import ImageOps
from PIL import UnidentifiedImageError

from Lance.src.blender.main.base.base_converter import BaseConverter
from Lance.src.blender.main.utils.hash import sha256_bytes


class PngConverter(BaseConverter):
    """
    Minimal image-to-Lance converter.

    Row schema (per image):
      - _pk: string         # stable primary key (sha256 of PNG bytes)
      - image: binary       # PNG-encoded bytes
      - height: int32
      - width: int32
      - channels: int32
      - format: string      # original format lowercased
      - label: int32        # inferred if using folder classification (else -1)
      - class: string       # folder name if available, else ""
      - uri: string         # relative or absolute path
      - sha256: string      # equals _pk
    """

    # ---------- single file -> dict ----------
    def _convert_impl(self, source: str) -> Dict[str, Any]:
        """Convert one file to canonical PNG bytes + metadata (no write)."""
        with open(source, "rb") as f:
            raw = f.read()

        # EXIF-aware orientation handling for JPEGs, etc.
        with Image.open(io.BytesIO(raw)) as im0:
            im = ImageOps.exif_transpose(im0)
            w, h = im.size
            ch = len(im.getbands())
            fmt = (im.format or "png").lower()

            # Re-encode to PNG for deterministic storage & hashing
            bio = io.BytesIO()
            im.save(bio, format="PNG")
            png_bytes = bio.getvalue()

        sha = sha256_bytes(png_bytes)
        return {
            "uri": source,
            "data": png_bytes,
            "height": h,
            "width": w,
            "channels": ch,
            "format": fmt,
            "sha256": sha,
            "_pk": sha,
        }

    @staticmethod
    def _find_images(
        root: str,
        suffixes: Tuple[str, ...],
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
        Recursively scan `root` and return a list of valid image files.

        - First filter by file extension (case-insensitive).
        - Optionally open each candidate with PIL and call `verify()` to ensure it's a real image.
        - Skip hidden files/dirs and common noise dirs by default.
        - Deterministic order (sorted paths).
        """
        sufs = {s.lower() for s in suffixes}
        out: List[Path] = []

        # Walk with control over which dirs to descend into
        for dirpath, dirnames, filenames in os.walk(
            root, followlinks=follow_symlinks
        ):
            # prune dirs
            if ignore_hidden:
                dirnames[:] = [d for d in dirnames if not d.startswith(".")]
            if exclude_dirs:
                dirnames[:] = [d for d in dirnames if d not in exclude_dirs]

            for name in filenames:
                if ignore_hidden and name.startswith("."):
                    continue
                p = Path(dirpath) / name
                if p.suffix.lower() not in sufs:
                    continue

                if validate:
                    try:
                        # Cheap integrity check without decoding full pixels
                        with Image.open(p) as im:
                            im.verify()
                    except (UnidentifiedImageError, OSError, ValueError):
                        # Not an image or corrupted
                        continue

                out.append(p)
                if limit is not None and len(out) >= limit:
                    # Early stop as soon as we have enough valid images
                    out.sort(key=lambda x: str(x).lower())
                    return out

        out.sort(key=lambda x: str(x).lower())
        return out

    @staticmethod
    def folder_to_table(
        dataset_dir: str,
        *,
        limit: Optional[int] = None,
        suffixes: Tuple[str, ...] = (
            ".png",
            ".jpg",
            ".jpeg",
            ".bmp",
            ".webp",
            ".tif",
            ".tiff",
        ),
        class_from_parent: bool = True,
        store_uri_relative: bool = True,
    ) -> pa.Table:
        """
        Scan a folder tree, convert all images to a Lance-ready Arrow table.
        Labels are inferred from parent folder name when class_from_parent=True.
        """
        conv = PngConverter()
        files = PngConverter._find_images(dataset_dir, suffixes)
        if limit is not None:
            files = files[:limit]

        imgs, h, w, c, fmts, uris, shas, pks, classes, labels = (
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
        )
        label_map: Dict[str, int] = {}
        next_label = 0
        base = Path(dataset_dir)

        for p in files:
            row = conv.convert(str(p))
            cls = p.parent.name if class_from_parent else ""

            if cls not in label_map:
                label_map[cls] = next_label
                next_label += 1

            imgs.append(row["data"])
            h.append(row["height"])
            w.append(row["width"])
            c.append(row["channels"])
            fmts.append(row["format"])
            sha = row["sha256"]
            shas.append(sha)
            pks.append(sha)
            classes.append(cls)
            labels.append(label_map[cls] if class_from_parent else -1)
            uris.append(
                os.path.relpath(str(p), str(base))
                if store_uri_relative
                else str(p)
            )

        return pa.table(
            {
                "_pk": pa.array(pks, type=pa.string()),
                "image": pa.array(imgs, type=pa.binary()),
                "height": pa.array(h, type=pa.int32()),
                "width": pa.array(w, type=pa.int32()),
                "channels": pa.array(c, type=pa.int32()),
                "format": pa.array(fmts, type=pa.string()),
                "label": pa.array(labels, type=pa.int32()),
                "class": pa.array(classes, type=pa.string()),
                "uri": pa.array(uris, type=pa.string()),
                "sha256": pa.array(shas, type=pa.string()),
            }
        )

    @staticmethod
    def write_lance(
        table: pa.Table,
        lance_uri: str,
        *,
        mode: Literal["overwrite", "append"] = "overwrite",
        storage_options: Optional[Dict[str, str]] = None,
        partition_cols: Optional[List[str]] = None,
    ) -> str:
        """Write an Arrow table into a Lance dataset."""
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

    @staticmethod
    def folder_to_lance(
        dataset_dir: str,
        lance_uri: str,
        *,
        limit: Optional[int] = None,
        overwrite: bool = True,
        storage_options: Optional[Dict[str, str]] = None,
        **scan_opts,
    ) -> str:
        """
        Convert an entire folder into a Lance dataset.
        """
        tbl = PngConverter.folder_to_table(
            dataset_dir, limit=limit, **scan_opts
        )
        mode = "overwrite" if overwrite else "append"
        return PngConverter.write_lance(
            tbl, lance_uri, mode=mode, storage_options=storage_options
        )

    # ---------- read back for PyTorch ----------
    @staticmethod
    def _decode_img(b: bytes) -> Image.Image:
        """Decode image bytes into PIL.Image with EXIF-aware orientation."""
        im = Image.open(io.BytesIO(b))
        return ImageOps.exif_transpose(im)

    @staticmethod
    def images_to_lance_from_arrow(
        split_dir: str,
        *,
        limit: Optional[int] = None,
        image_key_candidates: Tuple[str, ...] = ("image", "img"),
        label_key: str = "label",
    ) -> pa.Table:
        """
        Build a Lance-ready Arrow table from a HuggingFace datasets split on disk.
        Re-encodes to PNG, computes sha256 per image, keeps numeric label if present.

        Columns match folder_to_table:
          _pk, image, height, width, channels, format, label, class, uri, sha256
        """
        try:
            from datasets import load_from_disk
        except Exception as e:
            raise ImportError(
                "images_to_lance_from_arrow requires `datasets` package. "
                "pip install datasets"
            ) from e

        ds = load_from_disk(split_dir)
        n = len(ds) if limit is None else min(limit, len(ds))

        imgs, hh, ww, cc, fmts, labels, classes, uris, shas, pks = (
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
        )

        # iterate a sliced view for speed
        for ex in ds.select(range(n)):
            # find image field
            img_val = None
            for k in image_key_candidates:
                if k in ex:
                    img_val = ex[k]
                    break
            if img_val is None:
                raise KeyError(
                    f"No image column among {image_key_candidates} in example"
                )

            # to PIL
            if hasattr(img_val, "convert"):  # PIL-like
                pil = img_val
            else:
                # ndarray/other -> PIL
                pil = Image.fromarray(np.array(img_val))

            pil = ImageOps.exif_transpose(pil)
            w, h = pil.size
            ch = len(pil.getbands())
            fmt = (getattr(pil, "format", None) or "png").lower()

            # canonical PNG bytes for stable hashing
            bio = io.BytesIO()
            pil.save(bio, format="PNG")
            data = bio.getvalue()

            sha = sha256_bytes(data)

            imgs.append(data)
            hh.append(h)
            ww.append(w)
            cc.append(ch)
            fmts.append(fmt)
            labels.append(int(ex[label_key]) if label_key in ex else -1)
            classes.append(str(ex.get("class", "")))  # HF split通常无class，这里留空
            uris.append("")  # HF split通常无原始路径
            shas.append(sha)
            pks.append(sha)

        if not imgs:
            return pa.table({})

        return pa.table(
            {
                "_pk": pa.array(pks, type=pa.string()),
                "image": pa.array(imgs, type=pa.binary()),
                "height": pa.array(hh, type=pa.int32()),
                "width": pa.array(ww, type=pa.int32()),
                "channels": pa.array(cc, type=pa.int32()),
                "format": pa.array(fmts, type=pa.string()),
                "label": pa.array(labels, type=pa.int32()),
                "class": pa.array(classes, type=pa.string()),
                "uri": pa.array(uris, type=pa.string()),
                "sha256": pa.array(shas, type=pa.string()),
            }
        )

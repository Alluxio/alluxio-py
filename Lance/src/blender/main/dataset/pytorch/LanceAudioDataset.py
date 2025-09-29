from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import lance
import numpy as np
import torch
from torch.utils.data import Dataset


class LanceAudioDataset(Dataset):
    """
    Simple random-access dataset backed by a Lance dataset.
    Preloads audio bytes + (label, sr) into memory for fast training.
    """

    def __init__(
        self,
        lance_uri: str,
        *,
        transform=None,  # callable(wave: Tensor, sr: int) or callable(wave)
        target_sr: Optional[
            int
        ] = None,  # optional on-the-fly resample if you implement in transform
        storage_options: Optional[Dict[str, str]] = None,
        filter_expr: Optional[str] = None,
        extra_columns: Optional[List[str]] = None,
    ):
        self.transform = transform
        self.target_sr = target_sr

        ds = lance.dataset(lance_uri, storage_options=storage_options or {})
        cols = ["audio", "sample_rate", "label"]
        if extra_columns:
            cols = list(
                dict.fromkeys(
                    cols + [c for c in extra_columns if c in ds.schema.names]
                )
            )

        kw: Dict[str, Any] = {"columns": cols}
        if filter_expr:
            kw["filter"] = filter_expr
        tbl = ds.to_table(**kw)

        self._audio = tbl["audio"].to_pylist()
        self._sr = tbl["sample_rate"].to_pylist()
        self._label = (
            tbl["label"].to_pylist()
            if "label" in tbl.column_names
            else [-1] * len(self._audio)
        )

    def __len__(self) -> int:
        return len(self._audio)

    def __getitem__(self, idx: int):
        b = self._audio[idx]
        sr = int(self._sr[idx])
        y = np.frombuffer(b, dtype=np.float32)
        wave = torch.from_numpy(y)  # shape [N], mono float32

        if self.transform is not None:
            try:
                wave = self.transform(
                    wave, sr
                )  # e.g., torchaudio transforms expecting (wave, sr)
            except TypeError:
                wave = self.transform(wave)

        label = int(self._label[idx])
        return wave, label

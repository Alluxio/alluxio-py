from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional

import numpy as np
import torch
from torch.utils.data import get_worker_info
from torch.utils.data import IterableDataset

from ..core.mapping import ColumnMapping
from ..core.reader import LanceExampleReader


def _to_torch(obj: Any) -> Any:
    if isinstance(obj, np.ndarray):
        if obj.dtype == np.float32:
            return torch.from_numpy(obj)
        if obj.dtype in (np.int32, np.int64, np.uint8):
            return torch.from_numpy(obj)
        return torch.tensor(obj, dtype=torch.float32)
    if isinstance(obj, (int,)):
        return torch.tensor(obj, dtype=torch.long)
    if isinstance(obj, (float,)):
        return torch.tensor(obj, dtype=torch.float32)
    return obj


class LanceTorchDataset(IterableDataset):
    def __init__(
        self,
        lance_uri: str,
        mapping: ColumnMapping,
        tokenizer=None,
        batch_rows: int = 8192,
        filter: Optional[str] = None,
        columns: Optional[List[str]] = None,
        sample_transform: Optional[
            Callable[[Dict[str, torch.Tensor]], Dict[str, torch.Tensor]]
        ] = None,
    ):
        super().__init__()
        self.reader = LanceExampleReader(
            lance_uri=lance_uri,
            mapping=mapping,
            tokenizer=tokenizer,
            batch_rows=batch_rows,
            filter=filter,
            columns=columns,
        )
        self.sample_transform = sample_transform

    def __iter__(self) -> Iterator[Dict[str, torch.Tensor]]:
        info = get_worker_info()
        if info is None:
            for ex in self.reader:
                out = {k: _to_torch(v) for k, v in ex.items()}
                if self.sample_transform:
                    out = self.sample_transform(out)
                yield out
        else:
            wid, n = info.id, info.num_workers
            for idx, ex in enumerate(self.reader):
                if (idx % n) != wid:
                    continue
                out = {k: _to_torch(v) for k, v in ex.items()}
                if self.sample_transform:
                    out = self.sample_transform(out)
                yield out

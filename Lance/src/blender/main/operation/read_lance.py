from os import PathLike
from pathlib import Path
from typing import Iterable
from typing import List
from typing import Optional

import lance
import pyarrow as pa


class LanceReader:
    def __init__(self, path: PathLike):
        # Expect a dataset root dir (…/name.lance), not a fragment file
        p = Path(path)
        self.path = str(p)
        if not p.exists():
            raise FileNotFoundError(f"Lance dataset not found: {self.path}")
        if p.is_file():
            raise ValueError(
                f"Expected a Lance dataset directory (…/*.lance). "
                f"Got a file instead: {self.path}"
            )

    def read_all(self) -> pa.Table:
        ds = lance.dataset(self.path)
        return ds.to_table()

    def read_batches(
        self, batch_size: Optional[int] = None
    ) -> Iterable[pa.RecordBatch]:
        ds = lance.dataset(self.path)
        for batch in ds.to_batches(batch_size=batch_size):
            yield batch

    def read_columns(self, columns: List[str]) -> pa.Table:
        ds = lance.dataset(self.path)
        return ds.to_table(columns=columns)

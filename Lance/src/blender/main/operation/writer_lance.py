from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional

import lance
import pyarrow as pa


class LanceWriter:
    def __init__(
        self,
        path: str,
        mode: str = "overwrite",
        primary_key: Optional[str] = None,
    ):
        self.path = path
        self.mode = mode
        self.primary_key = primary_key

    def write_batch(self, records: Iterable[Dict[str, Any]]):
        # Write one batch to Lance at a time
        table = pa.Table.from_pylist(list(records))
        lance.write_dataset(
            table,
            self.path,
            mode=self.mode,
        )
        # The next batch will use "append"
        self.mode = "append"

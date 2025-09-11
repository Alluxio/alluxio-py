from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import pandas as pd
import pyarrow as pa


class BaseEngine(ABC):
    # Engine

    @abstractmethod
    def write(self, table: Union[pa.Table, pd.DataFrame], **kwargs) -> None:
        ...

    @abstractmethod
    def append(self, table: Union[pa.Table, pd.DataFrame], **kwargs) -> None:
        ...

    @abstractmethod
    def upsert(self, table: Union[pa.Table, pd.DataFrame], **kwargs) -> None:
        ...

    @abstractmethod
    def read(self, **kwargs) -> pa.Table:
        ...

    @abstractmethod
    def list_versions(self) -> List[Dict[str, Any]]:
        ...

    @abstractmethod
    def rollback(self, version: int) -> None:
        ...

    @abstractmethod
    def optimize(self) -> None:
        ...

    @abstractmethod
    def create_vector_index(self, column: str, **kwargs) -> None:
        ...

    @abstractmethod
    def query_sql(self, sql: str) -> pd.DataFrame:
        ...

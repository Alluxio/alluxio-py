import os
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import lance
import pandas as pd
import pyarrow as pa
from datafusion import SessionContext
from lance import FFILanceTableProvider
from lance import LanceDataset

from Lance.src.blender.main.utils.build_filter_expression import (
    _build_filter_expression,
)


class DatasetManager:
    def __init__(
        self,
        path: str,
        primary_key: Optional[str] = None,
        storage_options: Optional[Dict[str, str]] = None,
    ):
        self.path = path
        self.primary_key = primary_key
        os.makedirs(self.path, exist_ok=True)
        self.storage_options = storage_options or {}

    def write(
        self,
        table: Union[pa.Table, pd.DataFrame],
        mode: str = "overwrite",
        **lance_kwargs: Any,
    ) -> lance.LanceDataset:
        if isinstance(table, pd.DataFrame):
            table = pa.Table.from_pandas(table, preserve_index=False)
        ds = lance.write_dataset(table, self.path, mode=mode, **lance_kwargs)
        return ds

    def append(
        self, table: Union[pa.Table, pd.DataFrame], **lance_kwargs: Any
    ) -> None:
        self.write(table, mode="append", **lance_kwargs)

    def upsert(
        self, table: Union[pa.Table, pd.DataFrame], **lance_kwargs: Any
    ) -> LanceDataset:
        if not self.primary_key:
            raise ValueError("Upsert requires primary_key")
        if isinstance(table, pd.DataFrame):
            table = pa.Table.from_pandas(table, preserve_index=False)
        ds: LanceDataset = lance.dataset(self.path)
        (
            ds.merge_insert(on=self.primary_key)
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute(table)
        )
        return ds

    def overwrite(
        self, table: Union[pa.Table, pd.DataFrame], **lance_kwargs: Any
    ) -> LanceDataset:
        return self.write(table, mode="overwrite", **lance_kwargs)

    def merge(
        self,
        data_obj: Union[pa.Table, pd.DataFrame],
        left_on: str,
        right_on: Optional[str] = None,
        **lance_kwargs: Any,
    ) -> LanceDataset:
        if isinstance(data_obj, pd.DataFrame):
            data_obj = pa.Table.from_pandas(data_obj, preserve_index=False)
        right_on = right_on or left_on
        ds: LanceDataset = lance.dataset(self.path)
        ds.merge(data_obj, left_on=left_on, right_on=right_on, **lance_kwargs)
        return ds

    def list_versions(self) -> List[Dict[str, Any]]:
        ds: LanceDataset = lance.dataset(self.path)
        return ds.list_versions()

    def read(
        self,
        version: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[List[tuple]] = None,
    ) -> pa.Table:
        ds: LanceDataset = lance.dataset(self.path, version=version)
        kwargs: Dict[str, Any] = {}
        if columns is not None:
            kwargs["columns"] = columns
        if filters is not None:
            expr = _build_filter_expression(filters)
            kwargs["filter"] = expr
        return ds.to_table(**kwargs)

    # The rollback and lookup functions should be the same. During the rollback process, provide an option to decide whether to save the current modifications as a new version().
    def rollback(self, version: int) -> None:
        ds = lance.dataset(self.path, version=version)
        self.overwrite(ds.to_table())

    def get_row_at_version(self, key_value: Any, version: int) -> pa.Table:
        if not self.primary_key:
            raise ValueError("Row lookup requires primary_key")
        ds: LanceDataset = lance.dataset(self.path, version=version)
        expr = _build_filter_expression([(self.primary_key, "=", key_value)])
        return ds.to_table(filter=expr)

    def get_row_history(self, key_value: Any) -> Dict[int, pa.Table]:
        if not self.primary_key:
            raise ValueError("History lookup requires primary_key")
        history: Dict[int, pa.Table] = {}
        for vinfo in self.list_versions():
            ver = vinfo["version"]
            tbl = self.get_row_at_version(key_value, ver)
            if tbl.num_rows > 0:
                history[ver] = tbl
        return history

    def optimize(
        self,
        compact_target_rows: int = 8192,
        keep_last_n: Optional[int] = None,
        older_than_days: Optional[int] = None,
    ) -> None:
        import datetime

        ds: LanceDataset = lance.dataset(self.path)
        ds.optimize.compact_files(target_rows_per_fragment=compact_target_rows)
        cleanup_kwargs: Dict[str, Any] = {}
        if keep_last_n is not None:
            cleanup_kwargs["keep_last_n"] = keep_last_n
        if older_than_days is not None:
            cleanup_kwargs["older_than"] = datetime.timedelta(
                days=older_than_days
            )
        ds.cleanup_old_versions(
            **cleanup_kwargs
        ) if cleanup_kwargs else ds.cleanup_old_versions()

    def create_vector_index(
        self,
        column: str,
        index_type: str = "IVF_PQ",
        metric: str = "l2",
        rebuild: bool = False,
        **index_kwargs: Any,
    ) -> None:
        ds: LanceDataset = lance.dataset(self.path)
        ds.create_index(
            column,
            index_type=index_type,
            metric=metric,
            rebuild=rebuild,
            **index_kwargs,
        )

    def query_sql(self, sql: str) -> pd.DataFrame:
        ctx = SessionContext()
        ds = lance.dataset(self.path)
        provider = FFILanceTableProvider(ds)
        ctx.register_table_provider("ds", provider)
        df_table = ctx.sql(sql)
        return df_table.to_pandas()

    def __repr__(self) -> str:
        return f"<DatasetManager path={self.path!r} key={self.primary_key!r}>"

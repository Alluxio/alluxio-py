from __future__ import annotations

import datetime
import os
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import lance
import pandas as pd
import pyarrow as pa
from datafusion import SessionContext
from lance import FFILanceTableProvider
from lance import LanceDataset

# Filter tuple type: ("column", "operator", value)
Op = Tuple[str, str, Any]


# -------------------- Helper functions --------------------
def _q(v: Any) -> str:
    """Convert Python values to SQL literals for filter expressions."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _build_filter_expression(filters: List[Op]) -> str:
    """
    Build a DataFusion/Lance filter expression from a list of (col, op, val) tuples.
    Supported operators: =, !=, >, >=, <, <=, in, not in, like, ilike, is, is not

    Example:
      [("status","=","ok"), ("age",">",18), ("id","in",[1,2,3])]
    -> "(status = 'ok') AND (age > 18) AND (id IN (1,2,3))"
    """
    if not filters:
        return ""
    parts = []
    for col, op, val in filters:
        op_l = op.strip().lower()
        if op_l in {"in", "not in"}:
            if not isinstance(val, (list, tuple, set)):
                raise ValueError(f"Operator {op} requires list/tuple/set")
            vals = ", ".join(_q(x) for x in val)
            parts.append(
                f"({col} {'IN' if op_l=='in' else 'NOT IN'} ({vals}))"
            )
        elif op_l in {"like", "ilike"}:
            parts.append(
                f"({col} {'ILIKE' if op_l=='ilike' else 'LIKE'} {_q(val)})"
            )
        elif op_l in {"is", "is not"}:
            parts.append(
                f"({col} {'IS NOT' if op_l=='is not' else 'IS'} {_q(val)})"
            )
        elif op_l in {"=", "!=", ">", ">=", "<", "<="}:
            parts.append(f"({col} {op} {_q(val)})")
        else:
            raise ValueError(f"Unsupported operator: {op}")
    return " AND ".join(parts)


# -------------------- DatasetManager --------------------
class DatasetManager:
    """
    Lance dataset manager for a single dataset path.

    Features:
      - IO: write / append / upsert / merge / read
      - Queries: search (returns DataFrame), register (SQL table), query_sql
      - Row-level: lookup / get_row_at_version / get_row_history
      - Versioning: list_versions / rollback / forward_to
      - Maintenance: delete_rows / optimize / create_vector_index
    """

    def __init__(
        self,
        path: str,
        primary_key: Optional[str] = None,
        storage_options: Optional[Dict[str, str]] = None,
    ):
        """
        :param path: Lance dataset path (local dir, or s3:// / gs:// etc.)
        :param primary_key: column name to use as primary key (for upsert/lookup)
        :param storage_options: passed to lance.dataset/write_dataset
        """
        self.path = path
        self.primary_key = primary_key
        if self.path.startswith("/") or self.path.startswith("./"):
            os.makedirs(self.path, exist_ok=True)
        self.storage_options = storage_options or {}
        self._ctx = SessionContext()  # SQL context

    # ---------- IO ----------
    def write(
        self,
        table: Union[pa.Table, pd.DataFrame],
        mode: str = "overwrite",
        **lance_kwargs: Any,
    ) -> LanceDataset:
        """Write a new dataset, overwriting if it already exists."""
        if isinstance(table, pd.DataFrame):
            table = pa.Table.from_pandas(table, preserve_index=False)
        return lance.write_dataset(
            table,
            self.path,
            mode=mode,
            storage_options=self.storage_options,
            **lance_kwargs,
        )

    def append(
        self, table: Union[pa.Table, pd.DataFrame], **lance_kwargs: Any
    ) -> None:
        """Append rows to an existing dataset."""
        self.write(table, mode="append", **lance_kwargs)

    def upsert(
        self,
        table: Union[pa.Table, pd.DataFrame],
        **lance_kwargs: Any,
    ) -> LanceDataset:
        """
        Insert or update rows based on the primary_key.
        - If dataset does not exist, behaves like write(overwrite).
        - Requires primary_key to exist in the schema.
        """
        if not self.primary_key:
            raise ValueError("Upsert requires primary_key")
        if isinstance(table, pd.DataFrame):
            table = pa.Table.from_pandas(table, preserve_index=False)

        try:
            ds: LanceDataset = lance.dataset(
                self.path, storage_options=self.storage_options
            )
        except Exception:
            return self.write(table, mode="overwrite", **lance_kwargs)

        if self.primary_key not in ds.schema.names:
            raise ValueError(
                f"primary_key '{self.primary_key}' not in dataset schema"
            )

        (
            ds.merge_insert(on=self.primary_key)
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute(table)
        )
        return ds

    def merge(
        self,
        data: Union[pa.Table, pd.DataFrame],
        left_on: str,
        right_on: Optional[str] = None,
        **lance_kwargs: Any,
    ) -> LanceDataset:
        """Generic merge (not limited to primary key upsert)."""
        if isinstance(data, pd.DataFrame):
            data = pa.Table.from_pandas(data, preserve_index=False)
        ds: LanceDataset = lance.dataset(
            self.path, storage_options=self.storage_options
        )
        ds.merge(
            data, left_on=left_on, right_on=right_on or left_on, **lance_kwargs
        )
        return ds

    def read(
        self,
        version: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[List[Op]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> pa.Table:
        """Read as an Arrow Table (with version/columns/filters/order/limit)."""
        ds: LanceDataset = lance.dataset(
            self.path, version=version, storage_options=self.storage_options
        )
        kw: Dict[str, Any] = {}
        if columns:
            kw["columns"] = columns
        if filters:
            expr = _build_filter_expression(filters)
            if expr:
                kw["filter"] = expr
        if order_by:
            kw["order_by"] = order_by
        if limit is not None:
            kw["limit"] = limit
        return ds.to_table(**kw)

    # ---------- Queries ----------
    def search(
        self,
        filters: Optional[List[Op]] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        version: Optional[int] = None,
    ) -> pd.DataFrame:
        """Structured search, returns pandas.DataFrame."""
        tbl = self.read(
            version=version,
            columns=columns,
            filters=filters,
            limit=limit,
            order_by=order_by,
        )
        return tbl.to_pandas()

    def register(self, name: str = "ds") -> None:
        """
        Register dataset in SQL context under a name.
        Example:
          mgr.register("images")
          df = mgr.query_sql("SELECT count(*) FROM images")
        """
        ds = lance.dataset(self.path, storage_options=self.storage_options)
        provider = FFILanceTableProvider(ds)
        self._ctx.register_table_provider(name, provider)

    def query_sql(self, sql: str) -> pd.DataFrame:
        """Execute SQL on registered datasets, return pandas.DataFrame."""
        return self._ctx.sql(sql).to_pandas()

    # ---------- Row-level ----------
    def get_row(
        self, key_value: Any, version: Optional[int] = None
    ) -> pa.Table:
        """
        Lookup a row by primary key.
        - If version=None, search in the current head version.
        - If version is provided, search in that historical version.
        """
        if not self.primary_key:
            raise ValueError("get_row requires primary_key")
        ds: LanceDataset = lance.dataset(
            self.path,
            version=version,
            storage_options=self.storage_options,
        )
        expr = _build_filter_expression([(self.primary_key, "=", key_value)])
        return ds.to_table(filter=expr)

    def get_row_history(self, key_value: Any) -> Dict[int, pa.Table]:
        """
        Return the history of a row across all versions (version -> table).
        Only includes versions where the row exists.
        """
        if not self.primary_key:
            raise ValueError("get_row_history requires primary_key")
        hist: Dict[int, pa.Table] = {}
        for v in self.list_versions():
            ver = v["version"]
            t = self.get_row(key_value, version=ver)
            if t.num_rows > 0:
                hist[ver] = t
        return hist

    # ---------- Versioning ----------
    def list_versions(self) -> List[Dict[str, Any]]:
        """List all version metadata of the dataset."""
        ds: LanceDataset = lance.dataset(
            self.path, storage_options=self.storage_options
        )
        return ds.list_versions()

    def rollback(self, version: int) -> Tuple[int, int, int]:
        """
        Restore content from a historical version as a new head.

        :param version: historical version number to roll back to
        :return: (restored_from_version, old_head_version, new_head_version)
        """
        old_head = self.list_versions()[-1]["version"]
        ds = lance.dataset(
            self.path, version=version, storage_options=self.storage_options
        )
        snap = ds.to_table()

        # Always creates a new head version
        self.write(snap, mode="overwrite")
        new_head = self.list_versions()[-1]["version"]

        return version, old_head, new_head

    def forward_to(self, version: int) -> Tuple[int, int, int]:
        """Forward to a later version (semantic alias of rollback)."""
        return self.rollback(version)

    # ---------- Maintenance ----------
    def delete_rows(self, filters: List[Op]) -> None:
        """Hard delete rows by filter (creates a new version)."""
        pred = _build_filter_expression(filters)
        if not pred:
            raise ValueError("delete_rows requires non-empty predicate")
        ds: LanceDataset = lance.dataset(
            self.path, storage_options=self.storage_options
        )
        ds.delete(pred)

    def optimize(
        self,
        compact_target_rows: int = 8192,
        keep_last_n: Optional[int] = None,
        older_than_days: Optional[int] = None,
    ) -> None:
        """Optimize dataset: compact small files, cleanup old versions."""
        ds: LanceDataset = lance.dataset(
            self.path, storage_options=self.storage_options
        )
        ds.optimize.compact_files(target_rows_per_fragment=compact_target_rows)

        if keep_last_n is None and older_than_days is None:
            ds.cleanup_old_versions()
        else:
            kwargs: Dict[str, Any] = {}
            if keep_last_n is not None:
                kwargs["keep_last_n"] = keep_last_n
            if older_than_days is not None:
                kwargs["older_than"] = datetime.timedelta(days=older_than_days)
            ds.cleanup_old_versions(**kwargs)

    def create_vector_index(
        self,
        column: str,
        index_type: str = "IVF_PQ",
        metric: str = "l2",
        rebuild: bool = False,
        **index_kwargs: Any,
    ) -> None:
        """Create a vector index on a numeric/embedding column."""
        ds: LanceDataset = lance.dataset(
            self.path, storage_options=self.storage_options
        )
        ds.create_index(
            column,
            index_type=index_type,
            metric=metric,
            rebuild=rebuild,
            **index_kwargs,
        )

    # ---------- Misc ----------
    def __repr__(self) -> str:
        return f"<DatasetManager path={self.path!r} key={self.primary_key!r}>"

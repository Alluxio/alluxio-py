import base64
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import pandas as pd
import pyarrow as pa

from Lance.src.blender.main.base.base_engine import BaseEngine
from Lance.src.blender.main.utils.client import GrpcClient
from Lance.src.blender.main.utils.client import HttpClient
from Lance.src.blender.main.utils.client import WsClient


class RemoteBaseEngine(BaseEngine):
    """
    Thin remote engine talking to a dataset service over HTTP/gRPC/WebSocket.
    Sends/receives Arrow IPC streams for tables; uses JSON for control params.
    """

    def __init__(self, client: Union[HttpClient, GrpcClient, WsClient]):
        self.client = client

    # --------------- utils ---------------
    def _to_ipc_bytes(self, table) -> bytes:
        """Convert a pandas/Arrow table into Arrow IPC bytes."""
        if isinstance(table, pd.DataFrame):
            table = pa.Table.from_pandas(table, preserve_index=False)
        elif not isinstance(table, pa.Table):
            raise TypeError(
                "table must be a pandas.DataFrame or pyarrow.Table"
            )
        buf = pa.BufferOutputStream()
        with pa.ipc.new_stream(buf, table.schema) as writer:
            writer.write_table(table)
        return buf.getvalue().to_pybytes()

    def _from_ipc_bytes(self, b: bytes) -> pa.Table:
        """Deserialize Arrow IPC stream bytes to a Table."""
        return pa.ipc.open_stream(pa.py_buffer(b)).read_all()

    # --------------- write paths ---------------
    def write(
        self,
        table: Union[pa.Table, pd.DataFrame],
        mode: str = "overwrite",
        **options,
    ) -> None:
        """
        Generic write path (append/overwrite) via remote service.
        For upserts, prefer .upsert() instead of write(mode="upsert").
        """
        ipc_bytes = self._to_ipc_bytes(table)

        if isinstance(self.client, HttpClient):
            self.client.post(
                "/dataset/operation",
                {"mode": mode, "table_ipc": ipc_bytes, "options": options},
            )
        elif isinstance(self.client, GrpcClient):
            # Expecting signature: write(ipc_bytes: bytes, mode: str, **options)
            self.client.write(ipc_bytes, mode, **options)
        else:  # WsClient
            self.client.send(
                "operation",
                {"mode": mode, "table_ipc": ipc_bytes, "options": options},
            )

    def append(self, table: Union[pa.Table, pd.DataFrame], **options) -> None:
        """Append rows to the dataset."""
        self.write(table, mode="append", **options)

    def upsert(self, table: "pa.Table | pd.DataFrame", **options) -> None:
        """
        Upsert rows based on the dataset's primary key on the server side.
        Tries dedicated endpoint first; falls back to generic operation+mode='upsert'.
        """
        ipc_bytes = self._to_ipc_bytes(table)

        if isinstance(self.client, HttpClient):
            # HTTP/JSON cannot carry raw bytes -> base64 encode
            b64 = base64.b64encode(ipc_bytes).decode("ascii")
            payload = {"table_ipc": b64, "options": options or {}}
            try:
                self.client.post("/dataset/upsert", payload)
            except Exception:
                # Fallback for older servers
                self.client.post(
                    "/dataset/operation",
                    {
                        "mode": "upsert",
                        "table_ipc": b64,
                        "options": options or {},
                    },
                )

        elif isinstance(self.client, GrpcClient):
            # Prefer the dedicated RPC if available
            if hasattr(self.client, "upsert"):
                # expected: upsert(table_ipc: bytes, options: dict)
                self.client.upsert(ipc_bytes, options or {})
            else:
                # fallback to Write(mode="upsert")
                # expected signature: write(table_ipc: bytes, mode: str, options: dict)
                self.client.write(ipc_bytes, "upsert", options or {})

        else:  # WsClient
            # WS payload is JSON -> base64
            b64 = base64.b64encode(ipc_bytes).decode("ascii")
            payload = {"table_ipc": b64, "options": options or {}}
            try:
                self.client.send("upsert", payload)
            except Exception:
                self.client.send(
                    "operation",
                    {
                        "mode": "upsert",
                        "table_ipc": b64,
                        "options": options or {},
                    },
                )

    # --------------- read & query ---------------
    def read(
        self,
        version: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[List[Tuple[str, str, Any]]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> pa.Table:
        """
        Read as Arrow Table from remote.
        `filters` is recommended to be a list of (col, op, val) tuples to align with local API.
        """
        params = {
            k: v
            for k, v in {
                "version": version,
                "columns": columns,
                "filters": filters,
                "limit": limit,
                "order_by": order_by,
            }.items()
            if v is not None
        }

        if isinstance(self.client, HttpClient):
            resp = self.client.post("/dataset/read", {"params": params})
            return self._from_ipc_bytes(resp["table_ipc"])
        elif isinstance(self.client, GrpcClient):
            ipc = self.client.read(params)  # expecting raw bytes
            return self._from_ipc_bytes(ipc)
        else:
            resp = self.client.send("read", {"params": params})
            return self._from_ipc_bytes(resp["table_ipc"])

    def search(
        self,
        filters: Optional[List[Tuple[str, str, Any]]] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        version: Optional[int] = None,
    ) -> pd.DataFrame:
        """Structured search that returns a pandas.DataFrame."""
        tbl = self.read(
            version=version,
            columns=columns,
            filters=filters,
            limit=limit,
            order_by=order_by,
        )
        return tbl.to_pandas()

    def query_sql(self, sql: str) -> pd.DataFrame:
        """Run SQL remotely and return a pandas DataFrame."""
        if isinstance(self.client, HttpClient):
            resp = self.client.post("/dataset/sql", {"sql": sql})
            return pd.DataFrame(resp["data"], columns=resp["columns"])
        elif isinstance(self.client, GrpcClient):
            result = self.client.query_sql(sql)
            return pd.DataFrame(result["data"], columns=result["columns"])
        else:
            resp = self.client.send("sql", {"sql": sql})
            return pd.DataFrame(resp["data"], columns=resp["columns"])

    # --------------- versions ---------------
    def list_versions(self) -> List[Dict[str, Any]]:
        """List dataset versions from remote."""
        if isinstance(self.client, HttpClient):
            return self.client.post("/dataset/versions", {})
        elif isinstance(self.client, GrpcClient):
            return self.client.list_versions()
        else:
            return self.client.send("versions", {})

    def preview_version(self, version: int) -> pa.Table:
        """
        Return a historical snapshot (Arrow Table) WITHOUT modifying the dataset.
        Equivalent to read(version=version).
        """
        return self.read(version=version)

    def rollback(self, version: int) -> Tuple[int, int, int]:
        """
        Restore content from a historical version as a new head.
        Returns (restored_from_version, old_head_version, new_head_version).
        """
        if isinstance(self.client, HttpClient):
            resp = self.client.post("/dataset/rollback", {"version": version})
            # Server is expected to return {'restored_from': int, 'old_head': int, 'new_head': int}
            return resp["restored_from"], resp["old_head"], resp["new_head"]
        elif isinstance(self.client, GrpcClient):
            restored_from, old_head, new_head = self.client.rollback(version)
            return restored_from, old_head, new_head
        else:
            resp = self.client.send("rollback", {"version": version})
            return resp["restored_from"], resp["old_head"], resp["new_head"]

    def forward_to(self, version: int) -> Tuple[int, int, int]:
        """
        Semantically the same as rollback, but indicates moving forward to a later version.
        Returns (restored_from_version, old_head_version, new_head_version).
        """
        if isinstance(self.client, HttpClient):
            # Prefer a dedicated endpoint; fallback to rollback if server only exposes one
            try:
                resp = self.client.post(
                    "/dataset/forward", {"version": version}
                )
            except Exception:
                resp = self.client.post(
                    "/dataset/rollback", {"version": version}
                )
            return resp["restored_from"], resp["old_head"], resp["new_head"]
        elif isinstance(self.client, GrpcClient):
            if hasattr(self.client, "forward_to"):
                restored_from, old_head, new_head = self.client.forward_to(
                    version
                )
            else:
                restored_from, old_head, new_head = self.client.rollback(
                    version
                )
            return restored_from, old_head, new_head
        else:
            try:
                resp = self.client.send("forward", {"version": version})
            except Exception:
                resp = self.client.send("rollback", {"version": version})
            return resp["restored_from"], resp["old_head"], resp["new_head"]

    # --------------- maintenance ---------------
    def delete_rows(self, filters: List[Tuple[str, str, Any]]) -> None:
        """
        Hard-delete rows matching the filter. Server should create a new version.
        """
        payload = {"filters": filters}
        if isinstance(self.client, HttpClient):
            self.client.post("/dataset/delete", payload)
        elif isinstance(self.client, GrpcClient):
            # Expecting signature: delete_rows(filters: List[tuple], **opts)
            self.client.delete_rows(filters)
        else:
            self.client.send("delete", payload)

    def optimize(self, **opts) -> None:
        """Compact small files and/or clean up old versions remotely."""
        if isinstance(self.client, HttpClient):
            self.client.post("/dataset/optimize", opts)
        elif isinstance(self.client, GrpcClient):
            self.client.optimize(**opts)
        else:
            self.client.send("optimize", opts)

    def create_vector_index(self, column: str, **opts) -> None:
        """Create a vector index on the specified column remotely."""
        if isinstance(self.client, HttpClient):
            self.client.post("/dataset/index", {"column": column, **opts})
        elif isinstance(self.client, GrpcClient):
            # Expecting signature: create_vector_index(column: str, **opts)
            self.client.create_vector_index(column, **opts)
        else:
            self.client.send("index", {"column": column, **opts})

    # --------------- misc ---------------
    def __repr__(self) -> str:
        return f"<RemoteBaseEngine client={self.client.__class__.__name__}>"

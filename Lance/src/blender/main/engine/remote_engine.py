from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import pandas as pd
import pyarrow as pa

from Lance.src.blender.main.base.base_engine import BaseEngine
from Lance.src.blender.main.utils.client import GrpcClient
from Lance.src.blender.main.utils.client import HttpClient
from Lance.src.blender.main.utils.client import WsClient


class RemoteBaseEngine(BaseEngine):
    def __init__(self, client: Union[HttpClient, GrpcClient, WsClient]):
        self.client = client

    def write(
        self,
        table: Union[pa.Table, pd.DataFrame],
        mode: str = "overwrite",
        **options,
    ) -> None:
        if isinstance(table, pd.DataFrame):
            table = pa.Table.from_pandas(table, preserve_index=False)
        buf = pa.BufferOutputStream()
        with pa.ipc.new_stream(buf, table.schema) as writer:
            writer.write_table(table)
        ipc_bytes = buf.getvalue().to_pybytes()
        if isinstance(self.client, HttpClient):
            self.client.post(
                "/dataset/operation",
                {"mode": mode, "table_ipc": ipc_bytes, "options": options},
            )
        elif isinstance(self.client, GrpcClient):
            self.client.write(ipc_bytes, mode, options)
        else:
            self.client.send(
                "operation",
                {"mode": mode, "table_ipc": ipc_bytes, "options": options},
            )

    def append(self, table: Union[pa.Table, pd.DataFrame], **options) -> None:
        self.write(table, mode="append", **options)

    def upsert(self, table: Union[pa.Table, pd.DataFrame], **options) -> None:
        self.write(table, mode="upsert", **options)

    def read(
        self,
        version: Optional[int] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> pa.Table:
        params = {
            k: v
            for k, v in {
                "version": version,
                "columns": columns,
                "filters": filters,
            }.items()
            if v is not None
        }
        if isinstance(self.client, HttpClient):
            resp = self.client.post("/dataset/read", {"params": params})
            buf = pa.py_buffer(resp["table_ipc"])
            return pa.ipc.open_stream(buf).read_all()
        elif isinstance(self.client, GrpcClient):
            ipc = self.client.read(params)
            return pa.ipc.open_stream(pa.py_buffer(ipc)).read_all()
        else:
            resp = self.client.send("read", {"params": params})
            return pa.ipc.open_stream(
                pa.py_buffer(resp["table_ipc"])
            ).read_all()

    def list_versions(self) -> dict | dict[str, Any] | Any:
        if isinstance(self.client, HttpClient):
            return self.client.post("/dataset/versions", {})
        elif isinstance(self.client, GrpcClient):
            return self.client.list_versions()
        else:
            return self.client.send("versions", {})

    def rollback(self, version: int) -> None:
        if isinstance(self.client, HttpClient):
            self.client.post("/dataset/rollback", {"version": version})
        elif isinstance(self.client, GrpcClient):
            self.client.rollback(version)
        else:
            self.client.send("rollback", {"version": version})

    def optimize(self, **opts) -> None:
        if isinstance(self.client, HttpClient):
            self.client.post("/dataset/optimize", opts)
        elif isinstance(self.client, GrpcClient):
            self.client.optimize(opts)
        else:
            self.client.send("optimize", opts)

    def create_vector_index(self, column: str, **opts) -> None:
        if isinstance(self.client, HttpClient):
            self.client.post("/dataset/index", {"column": column, **opts})
        elif isinstance(self.client, GrpcClient):
            self.client.create_vector_index(column, opts)
        else:
            self.client.send("index", {"column": column, **opts})

    def query_sql(self, sql: str) -> pd.DataFrame:
        if isinstance(self.client, HttpClient):
            resp = self.client.post("/dataset/sql", {"sql": sql})
            return pd.DataFrame(resp["data"], columns=resp["columns"])
        elif isinstance(self.client, GrpcClient):
            result = self.client.query_sql(sql)
            return pd.DataFrame(result["data"], columns=result["columns"])
        else:
            resp = self.client.send("sql", {"sql": sql})
            return pd.DataFrame(resp["data"], columns=resp["columns"])

    def __repr__(self) -> str:
        return f"<RemoteEngine client={self.client.__class__.__name__}>"

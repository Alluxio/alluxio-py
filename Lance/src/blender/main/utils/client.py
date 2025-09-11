import json
from typing import Any
from typing import Dict
from typing import Optional
from typing import Union

import grpc
import requests
import websocket

from ..rpc import test_pb2 as pb2
from ..rpc import test_pb2_grpc as pb2_grpc


class HttpClient:
    def __init__(
        self,
        base_url: str,
        auth: Optional[Dict[str, str]] = None,
        api_key: Optional[str] = None,
        basic: Optional[tuple] = None,
        cert: Optional[tuple] = None,
        verify: Union[bool, str] = True,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        if auth and "bearer" in auth:
            self.session.headers.update(
                {"Authorization": f"Bearer {auth['bearer']}"}
            )
        if basic:
            self.session.auth = basic
        if api_key:
            self.session.headers.update({"X-Api-Key": api_key})
        if cert:
            self.session.cert = cert
        self.session.verify = verify

    def post(self, path: str, json_body: dict) -> dict:
        url = f"{self.base_url}/{path.lstrip('/')}"
        r = self.session.post(url, json=json_body)
        r.raise_for_status()
        return r.json()


class GrpcClient:
    def __init__(
        self, target: str, creds: grpc.ChannelCredentials | None = None
    ):
        self.channel = (
            grpc.secure_channel(target, creds)
            if creds
            else grpc.insecure_channel(target)
        )
        self.stub = pb2_grpc.DatasetServiceStub(self.channel)

    def write(self, table_ipc: bytes, mode: str, options: dict) -> None:
        req = pb2.WriteRequest(
            mode=mode, table_ipc=table_ipc, options=json.dumps(options)
        )
        self.stub.WriteDataset(req)

    def read(self, params: dict) -> bytes:
        req = pb2.ReadRequest(params=json.dumps(params))
        res = self.stub.ReadDataset(req)
        return res.table_ipc

    def list_versions(self) -> list[dict]:
        res = self.stub.ListVersions(pb2.VersionsRequest())
        return json.loads(res.versions_json)

    def rollback(self, version: int) -> None:
        self.stub.RollbackDataset(pb2.RollbackRequest(version=version))

    def optimize(self, opts: dict) -> None:
        self.stub.OptimizeDataset(
            pb2.OptimizeRequest(options=json.dumps(opts))
        )

    def create_vector_index(self, column: str, opts: dict) -> None:
        self.stub.CreateIndex(
            pb2.IndexRequest(column=column, options=json.dumps(opts))
        )

    def query_sql(self, sql: str) -> dict:
        # 这里就不会再“未解析”了
        res = self.stub.QuerySQL(pb2.SQLRequest(sql=sql))
        # 防御空字符串
        data = json.loads(res.data_json or "[]")
        return {"data": data, "columns": list(res.columns)}


class WsClient:
    def __init__(self, ws_url: str):
        self.ws = websocket.create_connection(ws_url)

    def send(self, action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        msg = json.dumps({"action": action, "payload": payload})
        self.ws.send(msg)
        resp = self.ws.recv()
        return json.loads(resp)

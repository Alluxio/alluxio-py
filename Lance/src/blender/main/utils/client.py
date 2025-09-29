from __future__ import annotations

import base64
import json
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import grpc
import requests
import websocket
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..proto import test_pb2 as pb2
from ..proto import test_pb2_grpc as pb2_grpc


# ----------------------------- helpers -----------------------------
def _b64(s: bytes) -> str:
    return base64.b64encode(s).decode("ascii")


def _encode_for_json(obj: Any) -> Any:
    """
    Recursively convert any bytes into base64-encoded str so that json.dumps will succeed.
    Keys are kept unchanged; server must know to decode these fields back to bytes.
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, bytes):
        return _b64(obj)
    if isinstance(obj, dict):
        return {k: _encode_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_encode_for_json(v) for v in obj]
    # Fallback: try to JSON-serialize via string
    try:
        json.dumps(obj)
        return obj
    except Exception:
        return str(obj)


# ============================= HTTP CLIENT =============================
class HttpClient:
    def __init__(
        self,
        base_url: str,
        auth: Optional[Dict[str, str]] = None,
        api_key: Optional[str] = None,
        basic: Optional[tuple] = None,
        cert: Optional[tuple] = None,
        verify: Union[bool, str] = True,
        timeout: float = 60.0,
        retry: Optional[Retry] = None,
    ):
        """
        :param base_url: service base URL
        :param auth: e.g. {"bearer": "..."}
        :param api_key: value for X-Api-Key
        :param basic: (user, pass) for HTTP Basic
        :param cert: TLS client cert tuple for requests
        :param verify: TLS verification or CA bundle path
        :param timeout: default request timeout (seconds)
        :param retry: urllib3 Retry object for idempotent retries
        """
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.timeout = timeout

        # Auth headers
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

        # Retry policy for idempotent methods (we use POST primarily; apply with caution)
        if retry:
            adapter = HTTPAdapter(max_retries=retry)
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)

    def post(self, path: str, json_body: dict) -> dict:
        """
        POST JSON. Any bytes found inside json_body are base64-encoded automatically.
        Server must decode those fields (e.g., 'table_ipc').
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        safe_body = _encode_for_json(json_body)

        r = self.session.post(url, json=safe_body, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            # Attach response text for easier debugging
            raise requests.HTTPError(f"{e}\nResponse text: {r.text}") from e

        # Some endpoints may return non-JSON (rare). Try JSON first.
        ctype = r.headers.get("Content-Type", "")
        if (
            "application/json" in ctype
            or r.text.startswith("{")
            or r.text.startswith("[")
        ):
            return r.json()
        return {"raw": r.content}


# ============================= GRPC CLIENT =============================
class GrpcClient:
    def __init__(
        self, target: str, creds: Optional[grpc.ChannelCredentials] = None
    ):
        """
        :param target: e.g. 'localhost:50051'
        :param creds: grpc.ssl_channel_credentials(...) or None for insecure
        """
        self.channel = (
            grpc.secure_channel(target, creds)
            if creds
            else grpc.insecure_channel(target)
        )
        self.stub = pb2_grpc.DatasetServiceStub(self.channel)

    # -------- dataset IO --------
    def write(self, table_ipc: bytes, mode: str, options: dict) -> None:
        req = pb2.WriteRequest(
            mode=mode,
            table_ipc=table_ipc,
            options=json.dumps(options or {}),
        )
        self.stub.WriteDataset(req)

    def upsert(self, table_ipc: bytes, options: dict) -> None:
        req = pb2.UpsertRequest(
            table_ipc=table_ipc,
            options=json.dumps(options or {}),
        )
        self.stub.UpsertDataset(req)

    def read(self, params: dict) -> bytes:
        req = pb2.ReadRequest(params=json.dumps(params))
        res = self.stub.ReadDataset(req)
        return res.table_ipc

    # -------- versions --------
    def list_versions(self) -> List[Dict[str, Any]]:
        res = self.stub.ListVersions(pb2.VersionsRequest())
        return json.loads(res.versions_json or "[]")

    def rollback(self, version: int) -> Tuple[int, int, int]:
        """
        Expect server to return triplet; otherwise adapt here.
        """
        # Newer API
        if hasattr(self.stub, "RollbackEx"):
            res = self.stub.RollbackEx(pb2.RollbackRequest(version=version))
            return res.restored_from, res.old_head, res.new_head
        # Legacy: no return values
        self.stub.RollbackDataset(pb2.RollbackRequest(version=version))
        # If legacy, you can additionally call ListVersions before/after to compose tuple.
        return (-1, -1, -1)

    def forward_to(self, version: int) -> Tuple[int, int, int]:
        if hasattr(self.stub, "ForwardTo"):
            res = self.stub.ForwardTo(pb2.RollbackRequest(version=version))
            return res.restored_from, res.old_head, res.new_head
        # Fallback to rollback semantics if server has only Rollback
        return self.rollback(version)

    # -------- maintenance --------
    def optimize(self, **opts) -> None:
        self.stub.OptimizeDataset(
            pb2.OptimizeRequest(options=json.dumps(opts))
        )

    def create_vector_index(self, column: str, **opts) -> None:
        self.stub.CreateIndex(
            pb2.IndexRequest(column=column, options=json.dumps(opts))
        )

    def delete_rows(self, filters: List[Tuple[str, str, Any]]) -> None:
        if not hasattr(self.stub, "DeleteRows"):
            raise NotImplementedError(
                "DeleteRows RPC not implemented on server"
            )
        req = pb2.DeleteRequest(filters_json=json.dumps(filters))
        self.stub.DeleteRows(req)

    # -------- SQL --------
    def query_sql(self, sql: str) -> Dict[str, Any]:
        res = self.stub.QuerySQL(pb2.SQLRequest(sql=sql))
        data = json.loads(res.data_json or "[]")
        return {"data": data, "columns": list(res.columns)}


# ============================= WEBSOCKET CLIENT =============================
class WsClient:
    def __init__(self, ws_url: str, timeout: float = 60.0):
        """
        Simple JSON-over-WebSocket client. Any bytes inside payload are base64-encoded.
        """
        self.ws = websocket.create_connection(ws_url, timeout=timeout)

    def send(self, action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        safe_payload = _encode_for_json(payload)
        msg = json.dumps({"action": action, "payload": safe_payload})
        self.ws.send(msg)
        resp = self.ws.recv()
        return json.loads(resp)

    def close(self):
        try:
            self.ws.close()
        except Exception:
            pass

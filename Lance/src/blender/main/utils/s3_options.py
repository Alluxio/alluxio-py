from __future__ import annotations

import os
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Mapping
from typing import Optional
from typing import Tuple


@dataclass(frozen=True)
class S3Config:
    endpoint: Optional[str] = None
    region: Optional[str] = None
    key: Optional[str] = None
    secret: Optional[str] = None
    session_token: Optional[str] = None
    path_style: Optional[bool] = None  # True=path-style, False/None=auto
    use_ssl: Optional[bool] = None

    multipart_chunksize_mb: Optional[int] = None
    max_concurrency: Optional[int] = None
    max_retries: Optional[int] = None
    connect_timeout: Optional[float] = None
    read_timeout: Optional[float] = None

    # SSE/KMS
    sse: Optional[str] = None  # "AES256" 或 "aws:kms"
    sse_kms_key_id: Optional[str] = None


def _config_from_env() -> S3Config:
    endpoint = os.getenv("S3_ENDPOINT_URL") or os.getenv("AWS_S3_ENDPOINT")
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    key = os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    token = os.getenv("AWS_SESSION_TOKEN")

    pstyle_env = os.getenv("S3_USE_PATH_STYLE", "true").lower()
    path_style = True if pstyle_env in ("1", "true", "yes") else False

    use_ssl = (endpoint or "").startswith("https")

    chsz_mb = os.getenv("S3_MULTIPART_CHUNK_SIZE_MB")
    concurrency = os.getenv("S3_MAX_CONCURRENCY")
    retries = os.getenv("S3_MAX_RETRIES")
    cto = os.getenv("S3_CONNECT_TIMEOUT")
    rto = os.getenv("S3_READ_TIMEOUT")

    sse = os.getenv("S3_SSE")  # "AES256" or "aws:kms"
    kms = os.getenv("S3_SSE_KMS_KEY_ID")

    return S3Config(
        endpoint=endpoint or None,
        region=region or None,
        key=key or None,
        secret=secret or None,
        session_token=token or None,
        path_style=path_style,
        use_ssl=use_ssl if endpoint else None,
        multipart_chunksize_mb=int(chsz_mb) if chsz_mb else None,
        max_concurrency=int(concurrency) if concurrency else None,
        max_retries=int(retries) if retries else None,
        connect_timeout=float(cto) if cto else None,
        read_timeout=float(rto) if rto else None,
        sse=sse or None,
        sse_kms_key_id=kms or None,
    )


def _merge_config(
    base: S3Config, override: Mapping[str, Any] | None
) -> S3Config:
    if not override:
        return base
    curr = asdict(base)
    for k, v in override.items():
        if k in curr and v is not None:
            curr[k] = v
    return S3Config(**curr)


def _clean(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v not in (None, {}, [], ())}


def build_storage_options(cfg: S3Config) -> Dict[str, Any]:

    client_kwargs = _clean(
        {
            "region_name": cfg.region,
            "endpoint_url": cfg.endpoint,
        }
    )

    config_kwargs = {}
    if cfg.path_style is not None:
        config_kwargs = {
            "s3": {"addressing_style": "path" if cfg.path_style else "auto"}
        }

    s3_additional_kwargs = {}
    if cfg.sse:
        s3_additional_kwargs["ServerSideEncryption"] = cfg.sse
    if cfg.sse_kms_key_id:
        s3_additional_kwargs["SSEKMSKeyId"] = cfg.sse_kms_key_id

    s3fs_opts: Dict[str, Any] = {}
    if cfg.multipart_chunksize_mb:
        s3fs_opts["multipart_chunksize"] = (
            cfg.multipart_chunksize_mb * 1024 * 1024
        )
    if cfg.max_concurrency:
        s3fs_opts["max_concurrency"] = cfg.max_concurrency
    if cfg.max_retries is not None:
        s3fs_opts["retries"] = cfg.max_retries
    if cfg.connect_timeout is not None:
        s3fs_opts["connect_timeout"] = cfg.connect_timeout
    if cfg.read_timeout is not None:
        s3fs_opts["read_timeout"] = cfg.read_timeout

    opts = {
        "key": cfg.key,
        "secret": cfg.secret,
        "token": cfg.session_token,
        "client_kwargs": client_kwargs,
        "config_kwargs": config_kwargs or None,
        "s3_additional_kwargs": s3_additional_kwargs or None,  # SSE/KMS
        "use_ssl": cfg.use_ssl,
        **(_clean(s3fs_opts)),
    }
    opts["client_kwargs"] = _clean(opts.get("client_kwargs", {}))
    if "config_kwargs" in opts and not opts["config_kwargs"]:
        opts.pop("config_kwargs", None)
    if "s3_additional_kwargs" in opts and not opts["s3_additional_kwargs"]:
        opts.pop("s3_additional_kwargs", None)
    return _clean(opts)


_CACHE_LOCK = threading.Lock()
_STORAGE_CACHE: Dict[Tuple, Tuple[Dict[str, Any], float]] = {}

_DEFAULT_TTL = 3600  # s


def _cache_key(cfg: S3Config) -> Tuple:
    return tuple(asdict(cfg).items())


def get_s3_storage_options(
    *,
    override: Optional[Mapping[str, Any]] = None,
    ttl: Optional[int] = _DEFAULT_TTL,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    base = _config_from_env()
    cfg = _merge_config(base, override)

    key = _cache_key(cfg)
    now = time.time()

    with _CACHE_LOCK:
        if not force_refresh and key in _STORAGE_CACHE:
            opts, ts = _STORAGE_CACHE[key]
            if ttl is None or (now - ts) < ttl:
                return opts

        opts = build_storage_options(cfg)
        _STORAGE_CACHE[key] = (opts, now)
        return opts


def invalidate_s3_cache() -> None:
    with _CACHE_LOCK:
        _STORAGE_CACHE.clear()


@contextmanager
def temporary_s3_options(
    temp_override: Mapping[str, Any], ttl: Optional[int] = _DEFAULT_TTL
):
    """
    Usage：
        with temporary_s3_options({"endpoint": "http://127.0.0.1:9000"}):
    """
    # It is achieved through the method of temporarily injecting environment variables: only the necessary keys are overwritten; the context is exited to restore.
    ENV_KEYS = {
        "endpoint": "S3_ENDPOINT_URL",
        "region": "AWS_REGION",
        "key": "AWS_ACCESS_KEY_ID",
        "secret": "AWS_SECRET_ACCESS_KEY",
        "session_token": "AWS_SESSION_TOKEN",
        "path_style": "S3_USE_PATH_STYLE",
    }
    backup = {}
    try:
        # 备份并注入
        for k, envk in ENV_KEYS.items():
            if k in temp_override:
                backup[envk] = os.getenv(envk)
                v = temp_override[k]
                if isinstance(v, bool):
                    os.environ[envk] = "true" if v else "false"
                elif v is None:
                    if envk in os.environ:
                        del os.environ[envk]
                else:
                    os.environ[envk] = str(v)
        # 失效缓存后重新获取
        invalidate_s3_cache()
        yield
    finally:
        # 还原环境
        for envk, old in backup.items():
            if old is None:
                os.environ.pop(envk, None)
            else:
                os.environ[envk] = old
        invalidate_s3_cache()


# TODO: 当输入不合法的时候需要报错或者自动调整
def build_s3_uri(bucket: str, *keys: str) -> str:
    bucket = bucket.strip("/")
    if not keys:
        return f"s3://{bucket}"
    joined = "/".join([bucket] + [k.strip("/") for k in keys])
    return f"s3://{joined}"

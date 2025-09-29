import os
from typing import Any
from typing import Dict
from typing import Optional
from typing import Union

import requests
import transformers
from huggingface_hub import snapshot_download

from Lance.src.blender.main.base.base_downloader import BaseDownloader
from Lance.src.blender.main.model.result.arrow_result import ArrowResult
from Lance.src.blender.main.model.result.repo_result import RepoResult
from Lance.src.blender.main.model.result.transformers_result import (
    TransformersResult,
)


class ModelDownloader(BaseDownloader):
    def __init__(
        self,
        name: str,
        cache_dir: Optional[str] = None,
        *,
        mode: str = "repo",  # "repo" | "transformers" | "url"
        model_name: str = "AutoModel",
        tokenizer: str = "AutoTokenizer",
        use_fast_tokenizer: bool = True,
        trust_remote_code: bool = False,
        revision: Optional[str] = None,
        token: Optional[str] = None,
        from_pretrained_kwargs: Optional[Dict[str, Any]] = None,
        timeout: int = 60,
    ):
        super().__init__(name, cache_dir)
        self.mode = mode
        self.model_name = model_name
        self.tokenizer = tokenizer
        self.use_fast_tokenizer = use_fast_tokenizer
        self.trust_remote_code = trust_remote_code
        self.revision = revision
        self.token = token
        self.from_pretrained_kwargs = from_pretrained_kwargs or {}
        self.timeout = timeout

    def _download_impl(
        self,
    ) -> Union[RepoResult, ArrowResult, TransformersResult]:
        if self.mode == "repo":
            local_dir = os.path.join(self.cache_dir or ".", "model_repo")
            path = snapshot_download(
                repo_id=self.name,
                repo_type="model",
                local_dir=local_dir,
                local_dir_use_symlinks=False,
                resume_download=True,
                revision=self.revision,
                token=self.token,
            )
            return {"kind": "repo", "path": path}

        if self.mode == "url":
            local_dir = os.path.join(self.cache_dir or ".", "model_url")
            os.makedirs(local_dir, exist_ok=True)
            filename = os.path.basename(self.name.split("?")[0]) or "model.pth"
            local_path = os.path.join(local_dir, filename)
            if not os.path.exists(local_path):
                with requests.get(
                    self.name, stream=True, timeout=self.timeout
                ) as r:
                    r.raise_for_status()
                    with open(local_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)
            return {"kind": "url", "path": local_path}

        if self.mode == "transformers":
            AutoModelCls = getattr(transformers, self.model_name)
            AutoTokCls = getattr(transformers, self.tokenizer)
            tok = AutoTokCls.from_pretrained(
                self.name,
                cache_dir=self.cache_dir,
                revision=self.revision,
                token=self.token,
                use_fast=self.use_fast_tokenizer,
                trust_remote_code=self.trust_remote_code,
            )
            model = AutoModelCls.from_pretrained(
                self.name,
                cache_dir=self.cache_dir,
                revision=self.revision,
                token=self.token,
                trust_remote_code=self.trust_remote_code,
                **self.from_pretrained_kwargs,
            )
            return {"kind": "transformers", "model": model, "tokenizer": tok}

        raise ValueError("mode must be 'repo', 'transformers', or 'url'")

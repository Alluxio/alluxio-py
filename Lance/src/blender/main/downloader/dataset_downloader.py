import os
from pathlib import Path
from typing import Optional

from datasets import DatasetDict
from datasets import load_dataset
from huggingface_hub import snapshot_download
from huggingface_hub.errors import HfHubHTTPError

from Lance.src.blender.main.base.base_downloader import BaseDownloader

DATA_EXTS = {
    ".parquet",
    ".json",
    ".jsonl",
    ".csv",
    ".tsv",
    ".txt",
    ".npy",
    ".npz",
    ".pt",
    ".pth",
    ".jpg",
    ".jpeg",
    ".png",
    ".bmp",
    ".gif",
    ".tif",
    ".tiff",
    ".webp",
    ".wav",
    ".mp3",
    ".flac",
    ".ogg",
}


class DatasetDownloader(BaseDownloader):
    def __init__(
        self,
        name: str,
        split: str = "train",
        cache_dir: Optional[str] = None,
        *,
        mode: str = "repo",  # "repo" 或 "arrow"
        **load_kwargs,
    ):
        super().__init__(name, cache_dir)
        self.split = split
        self.mode = mode
        self.load_kwargs = load_kwargs

    def _download_impl(self):
        os.makedirs(self.cache_dir or ".", exist_ok=True)

        if self.mode == "repo":
            repo_dir = os.path.join(self.cache_dir, "repo")
            try:
                path = snapshot_download(
                    repo_id=self.name,
                    repo_type="dataset",
                    local_dir=repo_dir,
                    local_dir_use_symlinks=False,
                    resume_download=True,
                )
                has_data_files = False
                for root, _, files in os.walk(path):
                    for fn in files:
                        if Path(fn).suffix.lower() in DATA_EXTS:
                            has_data_files = True
                            break
                    if has_data_files:
                        break
                return path
            except HfHubHTTPError as e:
                raise RuntimeError(f"snapshot_download 失败：{e}") from e

        elif self.mode == "arrow":
            out_root = os.path.join(self.cache_dir, "arrow")
            os.makedirs(out_root, exist_ok=True)

            ds = load_dataset(
                self.name, cache_dir=self.cache_dir, **self.load_kwargs
            )

            if isinstance(ds, DatasetDict):
                result_dirs = {}
                for sp, d in ds.items():
                    sp_dir = os.path.join(out_root, sp)
                    d.save_to_disk(sp_dir)
                    result_dirs[sp] = sp_dir
                return result_dirs
            else:
                sp_dir = os.path.join(out_root, self.split or "train")
                ds.save_to_disk(sp_dir)
                return sp_dir

        else:
            raise ValueError("mode 只支持 'repo' 或 'arrow'")

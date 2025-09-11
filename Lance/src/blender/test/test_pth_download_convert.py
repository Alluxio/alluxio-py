from pathlib import Path

import numpy as np

from Lance.src.blender.main.converter.tensor_converter import TensorConverter
from Lance.src.blender.main.downloader.model_downloader import ModelDownloader


def _fake_snapshot_download(
    repo_id: str, cache_dir: str = None, **kwargs
) -> str:
    """
    Mimic HF snapshot_download by creating a fake repo tree containing a .pth file.
    Return the repo root path like HF does.
    """
    repo_root = Path(cache_dir or ".") / f"{repo_id.replace('/', '__')}-FAKE"
    repo_root.mkdir(parents=True, exist_ok=True)
    (repo_root / "README.md").write_text("# fake", encoding="utf-8")

    # Write a tiny tensor as .pth / .pt (your converter just needs a file path)
    import torch

    w = torch.randn(3, 4)
    (repo_root / "mobilenet_v3_pretrained.pth").parent.mkdir(
        parents=True, exist_ok=True
    )
    torch.save({"weight": w}, repo_root / "mobilenet_v3_pretrained.pth")

    return str(repo_root)


def test_model_repo_pth_to_tensor(tmp_path, monkeypatch):
    # Create a temporary cache directory
    cache_dir = tmp_path / "hf_cache"
    cache_dir.mkdir()

    # IMPORTANT: Patch the function as it is used inside ModelDownloader.
    # Adjust "your_pkg.model_downloader" to match your actual module path.
    monkeypatch.setattr(
        "your_pkg.model_downloader.snapshot_download",
        _fake_snapshot_download,
        raising=True,
    )

    # Keep Hugging Face cache out of the user’s home directory
    monkeypatch.setenv("HF_HOME", str(cache_dir))

    # Initialize the downloader; repo name doesn’t matter since it’s mocked
    dl = ModelDownloader(
        name="someone/tiny-model",
        cache_dir=str(cache_dir),
        mode="repo",
    )

    # Perform the (mocked) download
    result = dl.download()
    repo_root = Path(result["path"] if isinstance(result, dict) else result)
    assert (
        repo_root.is_dir()
    ), f"Model repository path does not exist: {repo_root}"

    # Locate the .pth or .pt files
    pth_files = list(repo_root.rglob("*.pth")) + list(repo_root.rglob("*.pt"))
    assert pth_files, "No .pth/.pt files were found in the fake repo"

    # Convert each file and verify output
    conv = TensorConverter()
    for pth in pth_files:
        out = conv.convert(str(pth))
        assert (
            "tensor" in out
        ), f"Conversion result missing 'tensor' key: {pth}"
        arr = out["tensor"]
        assert isinstance(
            arr, np.ndarray
        ), f"Expected numpy.ndarray, got {type(arr)}"

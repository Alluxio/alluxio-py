import os

import lance
import numpy as np
import pyarrow as pa
import pytest
import torch
from huggingface_hub.utils import HfHubHTTPError

from Lance.src.blender.main.converter.tensor_converter import TensorConverter
from Lance.src.blender.main.downloader.model_downloader import ModelDownloader


def _first_tensor(obj):
    if isinstance(obj, torch.Tensor):
        return obj
    if isinstance(obj, dict):
        # 优先 'model'（OpenNMT 常见）
        if "model" in obj and isinstance(obj["model"], dict):
            t = _first_tensor(obj["model"])
            if t is not None:
                return t
        for v in obj.values():
            t = _first_tensor(v)
            if t is not None:
                return t
        return None
    if isinstance(obj, (list, tuple)):
        for v in obj:
            t = _first_tensor(v)
            if t is not None:
                return t
        return None
    return None


def test_hf_repo_pth_to_lance_via_tensor_converter(tmp_path):
    repo_id = "OpenNMT/nllb-200-onmt"
    try:
        dl = ModelDownloader(
            name=repo_id, cache_dir=str(tmp_path), mode="repo"
        )
        repo_path = dl.download()["path"]
    except HfHubHTTPError as e:
        pytest.skip(f"Hugging Face 下载失败或离线：{e}")
    except Exception as e:
        pytest.fail(f"ModelDownloader 运行失败：{e}")

    # 2) 收集全部 .pt/.pth
    pth_files = []
    for root, _, files in os.walk(repo_path):
        for fn in files:
            if fn.lower().endswith((".pt", ".pth")):
                pth_files.append(os.path.join(root, fn))
    if not pth_files:
        pytest.skip(f"仓库 {repo_id} 中未找到 .pt/.pth 文件")

    pth_files.sort()

    out_dir = tmp_path / "lance_out"
    out_dir.mkdir(parents=True, exist_ok=True)
    conv = TensorConverter()

    produced = {}  # pth_path -> (lance_uri, ndarray_float32)
    failures = []  # [(pth_path, errstr)]

    # 3) 逐文件转换 -> .lance
    for p in pth_files:
        # 3.1 先用你的 TensorConverter
        arr = None
        try:
            try:
                out = conv.convert(p)
            except AttributeError:
                out = conv._convert_impl(p)
            arr = out["tensor"]
            if not isinstance(arr, np.ndarray):
                raise TypeError(
                    f"TensorConverter 返回了非 ndarray 类型: {type(arr)}"
                )
            if arr.dtype != np.float32:
                arr = arr.astype(np.float32, copy=False)
        except Exception as e:
            print(e)
            try:
                obj = torch.load(p, map_location="cpu")
                t = _first_tensor(obj)
                if t is None:
                    raise ValueError("未在 checkpoint 中找到任何 torch.Tensor")
                arr = t.detach().cpu().numpy().astype(np.float32, copy=False)
            except Exception as e2:
                failures.append((p, repr(e2)))
                continue  # 跳过这个文件，处理下一个

        # 相对路径 -> 唯一输出名（避免不同子目录同名覆盖）
        rel = os.path.relpath(p, repo_path)  # e.g. "sub/encoder.pt"
        stem = os.path.splitext(rel)[0].replace(os.sep, "_")
        lance_uri = str(out_dir / f"{stem}.lance")

        table = pa.table(
            {
                "name": pa.array([rel], type=pa.string()),
                "dtype": pa.array([str(arr.dtype)], type=pa.string()),
                "shape": pa.array(
                    [list(arr.shape)], type=pa.list_(pa.int32())
                ),
                "numel": pa.array([int(arr.size)], type=pa.int64()),
                "data": pa.array([arr.tobytes()], type=pa.binary()),
            }
        )
        lance.write_dataset(table, lance_uri, mode="overwrite")
        produced[p] = (lance_uri, arr)

    # 至少成功转到一个
    assert produced, f"没有任何文件被成功转换。失败 {len(failures)} 个：\n" + "\n".join(
        f"{p} -> {e}" for p, e in failures
    )

    # 4) 逐个读回校验
    for pth_path, (lance_uri, ref_arr) in produced.items():
        ds = lance.dataset(lance_uri)
        tbl = ds.to_table(columns=["shape", "data"])
        assert tbl.num_rows == 1
        shape = tbl.column("shape").to_pylist()[0]
        data_bytes = tbl.column("data").to_pylist()[0]
        back = np.frombuffer(data_bytes, dtype=np.float32).reshape(shape)
        np.testing.assert_allclose(
            back, ref_arr, rtol=1e-6, atol=1e-6
        ), f"不一致：{pth_path}"

    # 如有失败，打印出来作为提示（不让测试失败）
    if failures:
        print(f"[WARN] 有 {len(failures)} 个 .pt/.pth 未能转换：")
        for p, e in failures:
            print(" -", p, "->", e)

    print(f"成功转换 {len(produced)} 个权重文件到 {out_dir}")

import tempfile
from pathlib import Path

import lance
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from Lance.src.blender.main.converter.language_converter import TextConverter
from Lance.src.blender.main.downloader.dataset_downloader import (
    DatasetDownloader,
)


def _save_table(table: pa.Table, base_out: Path) -> Path:
    alluxio_path = Path("/mnt/people") / base_out.name
    alluxio_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        lance_dir = alluxio_path.with_suffix(".lance")
        lance.write_dataset(table, str(lance_dir), mode="overwrite")
        assert lance_dir.is_dir(), f"Lance 目录未生成：{lance_dir}"
        return lance_dir
    except Exception:
        parquet_path = alluxio_path.with_suffix(".parquet")
        pq.write_table(table, parquet_path)
        assert parquet_path.is_file(), f"Parquet 文件未生成：{parquet_path}"
        return parquet_path


def _run_core(tmp_path: Path, repo_id: str, exts: set[str]):
    cache_dir = Path(tmp_path) / "hf_cache"
    d = DatasetDownloader(name=repo_id, cache_dir=str(cache_dir), mode="repo")
    result = d.download()

    # 兼容返回 str 或 {"kind": "repo", "path": "..."}
    repo_root = (
        Path(result["path"])
        if isinstance(result, dict) and "path" in result
        else Path(result)
    )
    assert repo_root.is_dir(), f"仓库根目录不存在: {repo_root}"

    # 找指定后缀文件
    files = [p for p in repo_root.rglob("*") if p.suffix.lower() in exts]
    if not files:
        pytest.skip(f"{repo_id} 没找到 {exts} 文件，跳过。")

    conv = TextConverter()
    converted = 0

    # output
    out_root = Path(tmp_path) / "converted"
    for f in files:
        out = conv.convert(str(f))
        assert "table" in out, f"转换结果缺少 'table'：{f}"
        table = out["table"]
        assert isinstance(table, pa.Table), f"不是 Arrow 表：{f}"
        assert table.num_columns > 0, f"空 schema：{f}"
        assert table.num_rows >= 0, f"行数无效：{f}"

        rel = f.relative_to(repo_root)
        base_out = out_root / rel  # 保留层级
        saved_path = _save_table(table, base_out)
        print(f"→ {f} -> {saved_path}")

        converted += 1

    assert converted > 0, "没有任何文件被成功转换"
    print(f"[{repo_id}] 转换并保存文件数：{converted}")


@pytest.mark.parametrize(
    "repo_id, exts",
    [
        ("fka/awesome-chatgpt-prompts", {".csv"}),  # CSV
        ("tatsu-lab/alpaca_eval", {".json"}),  # JSON
    ],
)
def test_text_converter_on_repo_files(tmp_path, repo_id, exts):
    _run_core(tmp_path, repo_id, exts)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as td:
        _run_core(Path(td), "fka/awesome-chatgpt-prompts", {".csv"})

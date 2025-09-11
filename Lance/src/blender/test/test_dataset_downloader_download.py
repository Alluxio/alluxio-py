import os
from pathlib import Path

import lance
import pyarrow as pa

from Lance.src.blender.main.converter.text_converter import TextConverter
from Lance.src.blender.main.downloader.dataset_downloader import (
    DatasetDownloader,
)


def test_mt_dataset_download_and_convert_to_lance(tmp_path):
    tmp_path = Path(tmp_path)
    cache_dir = str(tmp_path / "hf_cache")

    # 1) 下载仓库（包含一个 .json）
    downloader = DatasetDownloader(
        name="VanessaSchenkel/opus_books_en_pt",
        cache_dir=cache_dir,
        mode="repo",  # 以原始仓库文件形式下载
    )
    repo_path = downloader.download()
    assert os.path.isdir(cache_dir), "缓存目录未被创建"
    assert os.path.isdir(repo_path), f"下载路径不存在：{repo_path}"

    # 2) 在下载目录中查找第一个 .json 文件
    json_file = None
    for root, _, files in os.walk(repo_path):
        for fn in files:
            if fn.lower().endswith(".json"):
                json_file = os.path.join(root, fn)
                break
        if json_file:
            break
    assert json_file is not None, "未在仓库中找到 .json 文件"

    conv = TextConverter()
    try:
        out = conv.convert(json_file)  # 如果你的 BaseConverter 暴露 convert()
    except AttributeError:
        out = conv._convert_impl(json_file)  # 否则直接调用你给的 _convert_impl()
    table: pa.Table = out["table"]
    assert isinstance(table, pa.Table) and table.num_rows > 0, "转换后表为空"

    # （可选）限制写入行数，避免过大：默认 1000，可用环境变量 MAX_ROWS 覆盖
    max_rows = int(os.getenv("MAX_ROWS", "1000"))
    if table.num_rows > max_rows:
        table = table.slice(0, max_rows)

    # 4) 写成 .lance
    lance_uri = str(tmp_path / "opus_books_en_pt.lance")
    lance.write_dataset(table, lance_uri, mode="overwrite")

    # 5) 读回校验
    ds = lance.dataset(lance_uri)
    tbl = ds.to_table()
    assert tbl.num_rows > 0
    # 至少检查一下有文本列（具体列名依来源 JSON 而定，这里只做存在性校验）
    assert len(tbl.column_names) >= 1

    print("数据下载到：", repo_path)
    print("Lance 已写入：", lance_uri, "行数：", tbl.num_rows)

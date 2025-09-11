from Lance.src.blender.main.downloader.dataset_downloader import (
    DatasetDownloader,
)


def test_pytorch(tmp_path):
    # load dataset and model from local or remote
    cache_dir = str(tmp_path / "hf_cache")
    downloader = DatasetDownloader(
        "fka/awesome-chatgpt-prompts", cache_dir=cache_dir
    )
    result = downloader.download()

    assert isinstance(result, dict), "下载结果应为 dict"

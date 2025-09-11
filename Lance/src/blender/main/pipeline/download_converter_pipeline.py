from Lance.src.blender.main.base.base_converter import BaseConverter
from Lance.src.blender.main.base.base_downloader import BaseDownloader
from Lance.src.blender.main.operation.writer_lance import LanceWriter


# TODO: 未完成，整个pipeline有点问题
class DownloadConverterPipeline:
    def __init__(
        self,
        downloader: BaseDownloader,
        converter: BaseConverter,
        writer: LanceWriter,
        source_key: str,
    ):
        self.downloader = downloader
        self.converter = converter
        self.writer = writer
        self.source_key = source_key

    def run(self, batch_size: int = 1000):
        ds = self.downloader.download()  # returns a datasets.Dataset
        batch = []
        for example in ds:
            src = example[self.source_key]
            conv = self.converter._convert_impl(src)
            out = {k: v for k, v in example.items() if k != self.source_key}
            out.update(conv)
            batch.append(out)

            if len(batch) >= batch_size:
                self.writer.write_batch(batch)
                batch.clear()

        if batch:
            self.writer.write_batch(batch)

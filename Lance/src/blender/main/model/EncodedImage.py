class EncodedImage:
    def __init__(
        self,
        data: bytes,
        height: int,
        width: int,
        channels: int,
        fmt: str,
    ):
        self.data = data
        self.height = height
        self.width = width
        self.channels = channels
        self.format = fmt.lower()

    def to_pyarrow_struct(self):
        return {
            "data": self.data,
            "height": self.height,
            "width": self.width,
            "channels": self.channels,
            "format": self.format,
        }

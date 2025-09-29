import random
from typing import Dict
from typing import Iterator

from torch.utils.data import IterableDataset


class BufferedShuffle(IterableDataset):
    """
    基于固定大小 buffer 的局部随机打乱（近似洗牌）。
    """

    def __init__(self, src: IterableDataset, buffer_size: int = 10_000):
        self.src = src
        self.buffer_size = buffer_size

    def __iter__(self) -> Iterator[Dict]:
        buf = []
        it = iter(self.src)
        try:
            # 先填满缓冲
            for _ in range(self.buffer_size):
                buf.append(next(it))
        except StopIteration:
            pass

        random.shuffle(buf)
        for x in it:
            # 从缓冲随机弹出一个
            idx = random.randint(0, len(buf) - 1)
            yield buf[idx]
            buf[idx] = x

        # 倒出剩余
        while buf:
            idx = random.randint(0, len(buf) - 1)
            yield buf.pop(idx)

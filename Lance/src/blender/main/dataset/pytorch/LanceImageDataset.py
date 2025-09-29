import io
from typing import Any
from typing import Dict
from typing import Optional

import lance
import torch
from PIL import Image
from torch.utils.data import Dataset
from torchvision import transforms


class LanceImageDataset(Dataset):
    def __init__(
        self,
        uri: str,
        storage_options: Dict[str, Any],
        limit: Optional[int] = None,
    ):
        ds = lance.dataset(uri, storage_options=storage_options)
        t = ds.to_table(limit=limit) if limit else ds.to_table()
        rows = t.to_pylist()
        self.images = [r["image"] for r in rows]
        self.labels = [int(r["label"]) for r in rows]
        self.tfm = transforms.Compose(
            [
                transforms.Resize(256),
                transforms.CenterCrop(224),
                transforms.ToTensor(),
                transforms.Normalize(
                    [0.485, 0.456, 0.406], [0.229, 0.224, 0.225]
                ),
            ]
        )

    def __len__(self):
        return len(self.images)

    def __getitem__(self, i):
        img = Image.open(io.BytesIO(self.images[i])).convert("RGB")
        return self.tfm(img), torch.tensor(self.labels[i], dtype=torch.long)

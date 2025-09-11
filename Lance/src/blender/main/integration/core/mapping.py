from typing import List
from typing import Optional


class ColumnMapping:
    text: Optional[str] = None  # text
    image: Optional[str] = None  # uri or struct
    video: Optional[str] = None
    audio: Optional[str] = None

    features: Optional[List[str]] = None  # features
    label: Optional[str] = None  # label

    image_root: Optional[str] = None
    video_root: Optional[str] = None
    audio_root: Optional[str] = None

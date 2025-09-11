from typing import Literal
from typing import Mapping
from typing import TypedDict


class ArrowResult(TypedDict):
    kind: Literal["arrow"]
    splits: Mapping[str, str]  # split

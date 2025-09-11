from typing import Any
from typing import Literal
from typing import TypedDict


class TransformersResult(TypedDict):
    kind: Literal["transformers"]
    model: Any
    tokenizer: Any

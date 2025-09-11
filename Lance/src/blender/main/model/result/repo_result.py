from typing import Literal
from typing import TypedDict


class RepoResult(TypedDict):
    kind: Literal["repo"]
    path: str  # root_path

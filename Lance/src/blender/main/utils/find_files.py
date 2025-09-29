from pathlib import Path
from typing import List
from typing import Optional


def find_files(
    root: str, exts: List[str], limit: Optional[int] = None
) -> List[Path]:
    out: List[Path] = []
    for p in Path(root).rglob("*"):
        if p.is_file() and p.suffix.lower() in exts:
            out.append(p)
            if limit and len(out) >= limit:
                break
    return out

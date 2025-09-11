import json
import os
from typing import Any
from typing import Dict

import pandas as pd
import pyarrow as pa

from Lance.src.blender.main.base.base_converter import BaseConverter


class TextConverter(BaseConverter):
    def _convert_impl(self, source: str) -> Dict[str, Any]:
        ext = os.path.splitext(source)[1].lower()

        if ext == ".csv":
            df = pd.read_csv(source)
        elif ext == ".json":
            try:
                df = pd.read_json(source, lines=True)
            except ValueError:
                with open(source, "r", encoding="utf-8") as f:
                    data = json.load(f)
                df = pd.json_normalize(data)
        elif ext in (".xls", ".xlsx"):
            sheets = pd.read_excel(source, sheet_name=None)
            df = pd.concat(sheets.values(), ignore_index=True)
        else:
            raise ValueError(f"Unsupported file extension: {ext}")

        table = pa.Table.from_pandas(df, preserve_index=False)
        return {
            "uri": source,
            "table": table,
        }

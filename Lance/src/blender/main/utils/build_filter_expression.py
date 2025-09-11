from typing import List
from typing import Optional

import pyarrow as pa


def _build_filter_expression(
    filters: List[tuple],
) -> Optional[pa.compute.Expression]:
    expr: Optional[pa.compute.Expression] = None
    for col, op, val in filters:
        field = pa.compute.field(col)
        if op == "=":
            cond = field == val
        elif op == "!=":
            cond = field != val
        elif op == ">":
            cond = field > val
        elif op == ">=":
            cond = field >= val
        elif op == "<":
            cond = field < val
        elif op == "<=":
            cond = field <= val
        else:
            raise ValueError(f"Unsupported operator: {op}")
        expr = cond if expr is None else (expr & cond)
    return expr

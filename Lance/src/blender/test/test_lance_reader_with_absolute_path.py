from pathlib import Path

import pyarrow as pa

from Lance.src.blender.main.operation.read_lance import LanceReader


def test_lance_reader_with_absolute_path():
    dataset_root = Path("/mnt/people/alpaca_eval.lance")
    assert dataset_root.is_absolute()

    reader = LanceReader(dataset_root)

    table = reader.read_all()
    print(table)
    assert isinstance(table, pa.Table)
    assert table.num_columns > 0

    _ = next(reader.read_batches(batch_size=1024), None)

    cols = table.column_names
    print(cols)
    if cols:
        t2 = reader.read_columns([cols[0]])
        assert isinstance(t2, pa.Table)
        assert t2.num_columns == 1

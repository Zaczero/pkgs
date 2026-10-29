"""Feature-table admission, aligned edits and detached Arrow export."""

from __future__ import annotations

import time

from _observation import bootstrap, emit, prepare_measurement

if __name__ == '__main__':
    bootstrap()

import gometry as gm
import numpy as np
import pyarrow as pa
from _validation import emit_validation

ROWS = 100_000
ITERATIONS = 4


def input_table(rows: int) -> pa.Table:
    coordinates = np.arange(rows, dtype=np.float64)
    geometry = gm.points(coordinates, coordinates % 97).to_arrow()
    category = pa.array(np.arange(rows, dtype=np.int32) % 8).dictionary_encode()
    return pa.table({
        'shape': geometry,
        'category': category,
        'instant': pa.array(np.arange(rows, dtype=np.int64), type=pa.timestamp('ns')),
        'fid': pa.array(np.arange(rows, dtype=np.uint64)),
    }).replace_schema_metadata({b'gometry:feature_id': b'fid'})


def produce(table: pa.Table) -> pa.Table:
    features = gm.from_arrow(table)
    selected = features[::3]
    changed = selected.with_columns({
        'rank': np.arange(len(selected), dtype=np.int32),
    })
    return changed.materialize().to_arrow()


def validate_result(table: pa.Table) -> None:
    assert table.column_names == ['shape', 'category', 'instant', 'fid', 'rank']
    assert table.schema.metadata[b'gometry:feature_id'] == b'fid'
    assert table['instant'].type == pa.timestamp('ns')
    assert table['category'].type == pa.dictionary(pa.int32(), pa.int32())
    assert table['fid'].to_pylist() == [0, 3, 6]
    assert table['rank'].to_pylist() == [0, 1, 2]


def main() -> None:
    source = input_table(ROWS)
    result = produce(source)
    assert len(result) == (ROWS + 2) // 3
    prepare_measurement()
    started = time.perf_counter()
    for _ in range(ITERATIONS):
        result = produce(source)
    elapsed = time.perf_counter() - started
    assert len(result) == (ROWS + 2) // 3
    emit(
        metric='duration',
        raw_values=[elapsed],
        useful_work={'count': ITERATIONS * ROWS, 'unit': 'input-rows'},
        oracle_identity='case_feature_arrow_snapshot:typed-aligned-export',
        phase='steady',
        calibration_iterations=ITERATIONS,
    )


def validate_case() -> None:
    validate_result(produce(input_table(7)))
    emit_validation(
        'case_feature_arrow_snapshot',
        useful_output={'count': 7, 'unit': 'input-rows'},
    )


if __name__ == '__main__':
    import sys

    validate_case() if '--validate' in sys.argv else main()

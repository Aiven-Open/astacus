from collections.abc import Iterable
from itertools import islice
from typing import Any, TypeVar

import itertools
import pyarrow
import pyarrow.dataset as ds


def iterate_table(table: pyarrow.Table) -> Iterable[dict]:
    # Do as much computation as possible natively before doing this
    for batch in table.to_batches():
        for row in batch.to_pylist():
            yield row


def set_difference(array1: pyarrow.Array, array2: pyarrow.Array) -> pyarrow.Array:
    return _to_table(array1).join(_to_table(array2), keys=["id"], join_type="left anti").column("id")


def _to_table(array: pyarrow.Array) -> pyarrow.Table:
    return pyarrow.Table.from_arrays([array], names=("id",))


def convert_rows_to_dataset(rows: Iterable[tuple], schema: pyarrow.Schema, batch_size: int) -> ds.Dataset:
    return ds.dataset(convert_rows_to_record_batches(rows, schema, batch_size), schema)


def convert_rows_to_record_batches(
    rows: Iterable[tuple], schema: pyarrow.Schema, batch_size: int
) -> Iterable[pyarrow.RecordBatch]:
    for chunk in chunked(rows, batch_size):
        yield convert_rows_to_record_batch(chunk, schema)


def convert_rows_to_record_batch(rows: list[tuple], schema: pyarrow.Schema) -> pyarrow.RecordBatch:
    names = schema.names
    batch: dict[str, list[Any]] = {name: [] for name in names}
    for row in rows:
        for name, val in zip(names, row):
            batch[name].append(val)
    return pyarrow.RecordBatch.from_pydict(batch, schema=schema)


def constant_array(value: Any, length: int, type_: pyarrow.DataType | None = None) -> pyarrow.Array:
    return pyarrow.array(itertools.repeat(value, length), type=type_)


T = TypeVar("T")


def chunked(iterable: Iterable[T], size: int) -> Iterable[list[T]]:
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            return
        yield batch

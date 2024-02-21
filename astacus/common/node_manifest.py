from astacus.common import ipc
from typing import TypedDict

import base64
import pyarrow
import pyarrow.dataset as ds

BATCH_SIZE = 10_000


ARROW_SCHEMA = pyarrow.schema(
    [
        pyarrow.field("relative_path", pyarrow.string(), nullable=False),
        pyarrow.field("file_size", pyarrow.uint64(), nullable=False),
        pyarrow.field("mtime_ns", pyarrow.uint64(), nullable=False),
        pyarrow.field("hexdigest", pyarrow.binary(32), nullable=True),
        pyarrow.field("content", pyarrow.binary(), nullable=True),
    ]
)


class RowType(TypedDict):
    relative_path: str
    file_size: int
    mtime_ns: int
    hexdigest: bytes | None
    content: bytes | None


def convert_pydantic_to_row(row: ipc.SnapshotFile) -> RowType:
    return {
        "relative_path": row.relative_path,
        "file_size": row.file_size,
        "mtime_ns": row.mtime_ns,
        "hexdigest": None if row.hexdigest == "" else bytes.fromhex(row.hexdigest),
        "content": None if row.content_b64 is None else base64.b64decode(row.content_b64),
    }


def convert_row_to_pydantic(row: RowType) -> ipc.SnapshotFile:
    return ipc.SnapshotFile(
        relative_path=row["relative_path"],
        file_size=row["file_size"],
        mtime_ns=row["mtime_ns"],
        hexdigest="" if row["hexdigest"] is None else row["hexdigest"].hex(),
        content_b64=None if row["content"] is None else base64.b64encode(row["content"]).decode(),
    )


def combine_manifests(manifests: list[ds.Dataset], columns: list[str]) -> pyarrow.Table:
    tables = []
    for idx, manifest in enumerate(manifests):
        table = manifest.to_table(columns=columns).append_column("node", pyarrow.array([idx] * len(manifest)))
        tables.append(table)
    return pyarrow.concat_tables(tables)

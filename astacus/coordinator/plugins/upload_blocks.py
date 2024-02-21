# Bin packing for which hexdigests each node should upload.
from astacus.common import ipc
from astacus.common.pyarrow_utils import constant_array, iterate_table
from astacus.common.utils import AstacusModel
from collections.abc import Sequence, Set

import pyarrow
import pyarrow.compute as pc
import pyarrow.dataset as ds


class NodeIndexData(AstacusModel):
    node_index: int
    sshashes: list[ipc.SnapshotHash] = []
    total_size: int = 0

    def append_sshash(self, sshash: ipc.SnapshotHash) -> None:
        self.total_size += sshash.size
        self.sshashes.append(sshash)


def build_node_index_datas(
    *, hexdigests: Set[str], snapshots: Sequence[ipc.SnapshotResult], node_indices: Sequence[int]
) -> Sequence[NodeIndexData]:
    assert len(snapshots) == len(node_indices)
    sshash_to_node_indexes: dict[ipc.SnapshotHash, list[int]] = {}
    for i, snapshot_result in enumerate(snapshots):
        for snapshot_hash in snapshot_result.hashes or []:
            sshash_to_node_indexes.setdefault(snapshot_hash, []).append(i)

    node_index_datas = [NodeIndexData(node_index=node_index) for node_index in node_indices]

    # This is not really optimal algorithm, but probably good enough.

    # Allocate the things based on first off, how often they show
    # up (the least common first), and then reverse size order, to least loaded node.
    def _sshash_to_node_indexes_key(item):
        (sshash, indexes) = item
        return len(indexes), -sshash.size

    todo = sorted(sshash_to_node_indexes.items(), key=_sshash_to_node_indexes_key)
    for snapshot_hash, node_indexes in todo:
        if snapshot_hash.hexdigest in hexdigests:
            continue
        _, node_index = min((node_index_datas[node_index].total_size, node_index) for node_index in node_indexes)
        node_index_datas[node_index].append_sshash(snapshot_hash)
    return [data for data in node_index_datas if data.sshashes]


def build_node_index_datas_v2(*, node_datasets: list[ds.Dataset], existing_hexdigests: set[str]) -> Sequence[NodeIndexData]:
    n_nodes = len(node_datasets)
    files = get_table_to_upload(node_datasets, pyarrow.array(list(existing_hexdigests)))
    node_index_datas = [NodeIndexData(node_index=node_index) for node_index in range(n_nodes)]
    for file in iterate_table(files):
        hexdigest: str = file["hexdigest"]
        size = file["file_size"]
        sshash = ipc.SnapshotHash(hexdigest=hexdigest, size=size)
        nodes: list[int] = file["node_list"]
        _, node_index = min((node_index_datas[node_index].total_size, node_index) for node_index in nodes)
        node_index_datas[node_index].append_sshash(sshash)
    return [data for data in node_index_datas if data.sshashes]


def get_table_to_upload(node_datasets: list[ds.Dataset], existing_digests: pyarrow.StringArray) -> pyarrow.Table:
    tables = []
    for idx, dataset in enumerate(node_datasets):
        table = dataset.filter(pc.is_valid(dataset.field("hexdigest"))).to_table(columns=["hexdigest", "file_size"]).unique()
        node = constant_array(idx, len(table), type_=pyarrow.int32())
        table = table.append_column("node", node)
        tables.append(table)
    existing_digests_table = pyarrow.Table.from_pydict({"hexdigest": existing_digests})
    return (
        pyarrow.concat_tables(tables)
        .join(existing_digests_table, keys=["hexdigest"], join_type="left anti")
        .group_by("hexidigest")
        .aggregate([("file_size", "max"), ("node", "list")])
        .sort("file_size_max", "descending")
        .rename_columns(["hexdigest", "file_size", "node_list"])
    )

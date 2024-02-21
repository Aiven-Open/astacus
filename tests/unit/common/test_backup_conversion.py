from astacus.common import ipc
from astacus.common.backup_conversion import convert_backup
from binascii import hexlify


def test_convert_backup() -> None:
    backup = ipc.BackupManifest(
        start="2022-01-01T00:00:00Z",
        end="2022-01-02T00:00:00Z",
        attempt=1,
        plugin=ipc.Plugin.files,
        plugin_data={"a": 1},
        filename="abc123",
        upload_results=[
            ipc.SnapshotUploadResult(total_size=204, total_stored_size=3),
            ipc.SnapshotUploadResult(total_size=200, total_stored_size=6),
        ],
        snapshot_results=[
            ipc.SnapshotResult(
                start="2022-01-01T00:00:00Z",
                end="2022-01-02T00:00:00Z",
                files=3,
                total_size=204,
                state=ipc.SnapshotState(
                    root_globs=["a", "b"],
                    files=[
                        ipc.SnapshotFile(
                            relative_path="a",
                            file_size=100,
                            mtime_ns=2,
                            hexdigest="98ea6e4f216f2fb4b69fff9b3a44842c38686ca685f3f55dc48c5d3fb1107be4",
                            content_b64=None,
                        ),
                        ipc.SnapshotFile(
                            relative_path="b",
                            file_size=100,
                            mtime_ns=4,
                            hexdigest="98ea6e4f216f2fb4b69fff9b3a44842c38686ca685f3f55dc48c5d3fb1107be5",
                            content_b64=None,
                        ),
                        ipc.SnapshotFile(
                            relative_path="c/d",
                            file_size=4,
                            mtime_ns=4,
                            hexdigest="",
                            content_b64="YXNkZg==",
                        ),
                    ],
                ),
            ),
            ipc.SnapshotResult(
                start="2022-01-01T00:00:00Z",
                end="2022-01-02T00:00:00Z",
                files=2,
                total_size=200,
                state=ipc.SnapshotState(
                    files=[
                        ipc.SnapshotFile(
                            relative_path="a",
                            file_size=100,
                            mtime_ns=2,
                            hexdigest="5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
                            content_b64=None,
                        ),
                        ipc.SnapshotFile(
                            relative_path="b",
                            file_size=100,
                            mtime_ns=4,
                            hexdigest="e258d248fda94c63753607f7c4494ee0fcbe92f1a76bfdac795c9d84101eb317",
                            content_b64=None,
                        ),
                    ]
                ),
            ),
        ],
    )

    converted_backup, datasets = convert_backup(backup)

    assert converted_backup == ipc.BackupManifestV20240225(
        start="2022-01-01T00:00:00Z",
        end="2022-01-02T00:00:00Z",
        plugin=ipc.Plugin.files,
        attempt=1,
        snapshot_results=[
            ipc.SnapshotResultV20240225(
                start="2022-01-01T00:00:00Z",
                end="2022-01-02T00:00:00Z",
                files=3,
                total_size=204,
                node_id=0,
                root_globs=["a", "b"],
            ),
            ipc.SnapshotResultV20240225(
                start="2022-01-01T00:00:00Z",
                end="2022-01-02T00:00:00Z",
                files=2,
                total_size=200,
                node_id=1,
                root_globs=[],
            ),
        ],
        upload_results=[
            ipc.SnapshotUploadResult(total_size=204, total_stored_size=3),
            ipc.SnapshotUploadResult(total_size=200, total_stored_size=6),
        ],
        filename="abc123",
    )
    result_1 = datasets[0].to_table().to_pydict()
    assert result_1["relative_path"] == ["a", "b", "c/d"]
    assert result_1["file_size"] == [100, 100, 4]
    assert result_1["mtime_ns"] == [2, 4, 4]
    assert hexlify(result_1["hexdigest"][0]) == b"98ea6e4f216f2fb4b69fff9b3a44842c38686ca685f3f55dc48c5d3fb1107be4"
    assert hexlify(result_1["hexdigest"][1]) == b"98ea6e4f216f2fb4b69fff9b3a44842c38686ca685f3f55dc48c5d3fb1107be5"
    assert result_1["hexdigest"][2] is None
    assert result_1["content"] == [None, None, b"asdf"]

    result_2 = datasets[1].to_table().to_pydict()
    assert result_2["relative_path"] == ["a", "b"]
    assert result_2["file_size"] == [100, 100]
    assert result_2["mtime_ns"] == [2, 4]
    assert hexlify(result_2["hexdigest"][0]) == b"5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03"
    assert hexlify(result_2["hexdigest"][1]) == b"e258d248fda94c63753607f7c4494ee0fcbe92f1a76bfdac795c9d84101eb317"
    assert result_2["content"] == [None, None]

"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.node.progress import DebugProgress


def test_snapshot(snapshotter, storage):
    # Start with empty
    assert snapshotter.snapshot() == 0
    src = snapshotter.src
    dst = snapshotter.dst
    assert not (dst / "foo").is_file()

    # Create files in src, run snapshot
    snapshotter.create_2foobar()
    ss2 = snapshotter.get_snapshot_state()

    assert (dst / "foo").is_file()
    assert (dst / "foo").read_text() == "foobar"
    assert (dst / "foo2").read_text() == "foobar"

    blocks = snapshotter.get_snapshot_hashes()
    assert len(blocks) == 1
    assert len(blocks[0]) == 64  # in hex, so 32 bytes of hash = 256 bit hash

    while True:
        (src / "foo").write_text("barfoo")  # same length
        if snapshotter.snapshot() > 0:
            # Sometimes fails on first iteration(s) due to same mtime
            # (inaccurate timestamps)
            break
    ss3 = snapshotter.get_snapshot_state()
    assert ss2 != ss3
    assert snapshotter.snapshot() == 0
    assert (dst / "foo").is_file()
    assert (dst / "foo").read_text() == "barfoo"

    snapshotter.write_hashes_to_storage(hashes=blocks,
                                        storage=storage,
                                        progress=DebugProgress())

    # Remove file from src, run snapshot
    (src / "foo").unlink()
    assert snapshotter.snapshot() > 0
    assert snapshotter.snapshot() == 0
    assert not (dst / "foo").is_file()

    # Remove last file from src, run snapshot
    (src / "foo2").unlink()
    assert snapshotter.snapshot() > 0
    assert snapshotter.snapshot() == 0
    assert not (dst / "foo2").is_file()

    # Now shouldn't have any data blocks
    blocks_empty = snapshotter.get_snapshot_hashes()
    assert not blocks_empty

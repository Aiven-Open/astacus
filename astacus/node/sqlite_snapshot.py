"""Copyright (c) 2023 Aiven Ltd
See LICENSE for details.

"""

from astacus.common import magic
from astacus.common.ipc import SnapshotFile, SnapshotHash
from astacus.common.progress import Progress
from astacus.node.snapshot import Snapshot
from astacus.node.snapshotter import Snapshotter
from collections.abc import Iterable
from contextlib import closing
from functools import cached_property
from pathlib import Path
from typing import override

import logging
import os
import sqlite3

logger = logging.getLogger(__name__)


class SQLiteSnapshot(Snapshot):
    def __init__(self, dst: Path, db: Path) -> None:
        super().__init__(dst)
        self.db = db

    def __len__(self) -> int:
        return self._con.execute("select count(*) from snapshot_files;").fetchone()[0]

    def get_file(self, relative_path: str) -> SnapshotFile | None:
        cur = self._con.execute("select * from snapshot_files where relative_path = ?;", (relative_path,))
        row = cur.fetchone()
        return row_to_snapshotfile(row) if row else None

    def get_files_for_digest(self, hexdigest: str) -> Iterable[SnapshotFile]:
        return map(
            row_to_snapshotfile,
            self._con.execute(
                """
                select *
                from snapshot_files
                where hexdigest = ?
                order by relative_path;
                """,
                (hexdigest,),
            ),
        )

    def get_all_files(self) -> Iterable[SnapshotFile]:
        return map(row_to_snapshotfile, self._con.execute("select * from snapshot_files order by relative_path;"))

    @override
    def get_all_paths(self) -> Iterable[str]:
        return (row[0] for row in self._con.execute("select relative_path from snapshot_files order by relative_path;"))

    @override
    def get_total_size(self) -> int:
        return self._con.execute("select sum(file_size) from snapshot_files;").fetchone()[0] or 0

    def get_connection(self) -> sqlite3.Connection:
        return self._con

    def get_all_digests(self) -> Iterable[SnapshotHash]:
        for hexdigest, file_size in self._con.execute(
            """
            select hexdigest, file_size
            from snapshot_files
            where hexdigest != ''
            order by hexdigest;
            """
        ):
            yield SnapshotHash(hexdigest=hexdigest, size=file_size)

    @cached_property
    def _con(self) -> sqlite3.Connection:
        if self.db.exists():
            # We could probably use an old db again since everything should be
            # in a transaction, but there is little benefit so let's be safe and
            # just recreate it.
            self.db.unlink()
        else:
            self.db.parent.mkdir(parents=True, exist_ok=True)
        # Python 3.12+ fetch result are not consistent in multi-threading application
        # and causing an API misuse error.
        # The multithreading use is intended and supported because threadsafety is set to 3.
        # In serialized mode, SQLite can be safely used by multiple threads with no restriction.
        # Current solution is to set cached_statements=0 when connecting to the database.
        # See issue https://github.com/python/cpython/issues/118172
        assert sqlite3.threadsafety == 3, "sqlite is not multithreading safe."
        con = sqlite3.connect(self.db, isolation_level=None, check_same_thread=False, cached_statements=0)
        con.executescript(
            """
            begin;
            create table snapshot_files (
                relative_path text not null,
                file_size integer not null,
                mtime_ns integer not null,
                hexdigest text not null,
                content_b64 text,
                primary key (relative_path)
            );
            create index snapshot_files_hexdigest on snapshot_files(hexdigest);
            commit;
            """
        )
        return con


class SQLiteSnapshotter(Snapshotter[SQLiteSnapshot]):
    def perform_snapshot(self, *, progress: Progress) -> None:
        files = self._list_files_and_create_directories()
        with self._con:
            self._con.execute("begin")
            new_or_existing = self._compare_current_snapshot(files)
            for_upsert = self._compare_with_src(new_or_existing)
            with_digests = self._compute_digests(for_upsert)
            self._upsert_files(with_digests)
            self._con.execute("drop table if exists new_files;")

    def _list_files_and_create_directories(self) -> Iterable[str]:
        """List all files, and create directories in src."""
        logger.info("Listing files in %s", self._src)
        for dir_, _, files in os.walk(self._src):
            dir_path = Path(dir_)
            if any(parent.name == magic.ASTACUS_TMPDIR for parent in dir_path.parents):
                continue
            rel_dir = dir_path.relative_to(self._src)
            (self._dst / rel_dir).mkdir(parents=True, exist_ok=True)
            for f in files:
                rel_path = str(rel_dir / f)
                if not (dir_path / f).is_symlink() and self._groups.any_match(rel_path):
                    yield rel_path

    def _compare_current_snapshot(self, files: Iterable[str]) -> Iterable[tuple[str, SnapshotFile | None]]:
        with closing(self._con.cursor()) as cur:
            cur.execute(
                """
                create temporary table current_files (
                    relative_path text not null
                );
                """
            )
            cur.execute(
                """
                create temporary table new_files (
                    relative_path text not null,
                    file_size integer,
                    mtime_ns integer,
                    hexdigest text,
                    content_b64 text
                );
                """
            )
            cur.executemany("insert into current_files (relative_path) values (?);", ((f,) for f in files))
            cur.execute(
                """
                delete from snapshot_files
                where relative_path
                not in (select relative_path from current_files)
                returning relative_path;
                """
            )
            if not self._same_root_mode():
                logger.info("Deleting files in %s that are not in current snapshot", self._dst)
                for (relative_path,) in cur:
                    assert isinstance(relative_path, str)
                    (self._dst / relative_path).unlink(missing_ok=True)
            cur.execute(
                """
                insert into new_files
                select *
                from snapshot_files
                natural join current_files;
                """
            )
            cur.execute(
                """
                insert into new_files
                select relative_path, null, null, null, null
                from current_files
                where relative_path
                not in (
                    select relative_path
                    from snapshot_files
                );
                """
            )
            cur.execute("drop table current_files;")
            cur.execute(
                """
                select relative_path, file_size, mtime_ns, hexdigest, content_b64
                from new_files;
                """
            )
            for row in cur:
                if row[1] is None:
                    yield row[0], None
                else:
                    yield row[0], row_to_snapshotfile(row)

    def _compare_with_src(self, files: Iterable[tuple[str, SnapshotFile | None]]) -> Iterable[SnapshotFile]:
        logger.info("Checking metadata for files in %s", self._dst)
        for relpath, existing in files:
            try:
                new = self._file_in_src(relpath)
                if existing is None or not existing.underlying_file_is_the_same(new):
                    self._maybe_link(relpath)
                    yield new
            except FileNotFoundError:
                logger.warning("File %s disappeared while snapshotting", relpath)

    def _upsert_files(self, files: Iterable[SnapshotFile]) -> None:
        logger.info("Upserting files in snapshot db")
        self._con.executemany(
            """
            insert or replace
            into snapshot_files
                (relative_path, file_size, mtime_ns, hexdigest, content_b64)
            values (?, ?, ?, ?, ?);
            """,
            ((str(f.relative_path), f.file_size, f.mtime_ns, f.hexdigest, f.content_b64) for f in files),
        )

    def release(self, hexdigests: Iterable[str], *, progress: Progress) -> None:
        with self._con:
            self._con.execute("begin")
            with closing(self._con.cursor()) as cur:
                cur.execute(
                    """
                        create temporary table hexdigests (
                            hexdigest text not null
                        );
                        """
                )
                cur.executemany(
                    "insert into hexdigests (hexdigest) values (?);",
                    ((h,) for h in hexdigests if h != ""),
                )
                cur.execute(
                    """
                        select relative_path
                        from snapshot_files
                        where hexdigest in (select hexdigest from hexdigests);
                        """
                )
                for (relative_path,) in cur:
                    assert isinstance(relative_path, str)
                    (self._dst / relative_path).unlink(missing_ok=True)
                cur.execute("drop table hexdigests;")

    @property
    def _con(self) -> sqlite3.Connection:
        return self.snapshot.get_connection()


def row_to_path_and_snapshotfile(row: tuple) -> tuple[str, SnapshotFile | None]:
    return row[0], row_to_snapshotfile(row)


def row_to_snapshotfile(row: tuple) -> SnapshotFile:
    return SnapshotFile(relative_path=row[0], file_size=row[1], mtime_ns=row[2], hexdigest=row[3], content_b64=row[4])


def snapshotfile_to_row(file: SnapshotFile) -> tuple[str, int, int, str, str | None]:
    return (file.relative_path, file.file_size, file.mtime_ns, file.hexdigest, file.content_b64)

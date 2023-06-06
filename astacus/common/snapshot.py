"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from astacus.common.magic import DEFAULT_EMBEDDED_FILE_SIZE

import dataclasses


@dataclasses.dataclass(frozen=True, slots=True)
class SnapshotGroup:
    root_glob: str
    # None means "no limit": all files matching the glob will be embedded
    embedded_file_size_max: int | None = DEFAULT_EMBEDDED_FILE_SIZE

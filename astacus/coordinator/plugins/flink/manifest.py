"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""

from typing import Dict, Optional

import msgspec


class FlinkManifest(msgspec.Struct, kw_only=True, frozen=True):
    data: Optional[Dict] = msgspec.field(default_factory=dict)

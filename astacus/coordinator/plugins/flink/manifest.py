"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details.
"""

from astacus.common.utils import AstacusModel
from collections.abc import Mapping
from typing import Any


class FlinkManifest(AstacusModel):
    data: Mapping[str, Any] | None = {}

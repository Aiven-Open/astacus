"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from astacus.common.utils import AstacusModel
from collections.abc import Mapping
from typing import Any, Optional


class FlinkManifest(AstacusModel):
    data: Optional[Mapping[str, Any]] = {}

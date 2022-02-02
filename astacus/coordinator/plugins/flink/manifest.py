"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""

from astacus.common.utils import AstacusModel
from typing import Dict, Optional


class FlinkManifest(AstacusModel):
    data: Optional[Dict] = {}

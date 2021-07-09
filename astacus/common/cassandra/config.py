"""

Copyright (c) 2021 Aiven Ltd
See LICENSE for details

Client configuration data model

Note that resides in its own file mostly because it is imported in few
places in Astacus; the other modules in the directory have
dependencies on the actual cassandra driver, but this one does not.

"""

from astacus.common.utils import AstacusModel
from typing import List, Optional

SNAPSHOT_NAME = "astacus-backup"


class CassandraClientConfig(AstacusModel):
    # WhiteListRoundRobinPolicy contact points
    hostnames: List[str]

    port: int

    # PlainTextAuthProvider
    username: str
    password: str

    # If set, configure ssl access configuration which requires the ca cert
    ca_cert_path: Optional[str]

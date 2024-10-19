from .base import CoordinatorPlugin
from astacus.common.ipc import Plugin


def get_plugin(plugin: Plugin) -> type[CoordinatorPlugin]:
    if plugin == Plugin.cassandra:
        from .cassandra.plugin import CassandraPlugin

        return CassandraPlugin
    if plugin == Plugin.clickhouse:
        from .clickhouse.plugin import ClickHousePlugin

        return ClickHousePlugin
    if plugin == Plugin.files:
        from .files import FilesPlugin

        return FilesPlugin
    if plugin == Plugin.flink:
        from .flink.plugin import FlinkPlugin

        return FlinkPlugin
    if plugin == Plugin.m3db:
        from .m3db import M3DBPlugin

        return M3DBPlugin
    raise NotImplementedError

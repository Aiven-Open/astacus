from astacus.common.ipc import Plugin


def _get_plugin_info(name):
    # Some of the plugins (hello, Cassandra) have pretty hefty requirements.
    # Due to that, import actual modules only on demand.
    #
    # pylint: disable=import-outside-toplevel
    if name == Plugin.cassandra:
        from .cassandra import plugin_info as _cassandra_plugin_info
        return _cassandra_plugin_info
    if name == Plugin.m3db:
        from .m3db import plugin_info as _m3db_plugin_info
        return _m3db_plugin_info
    if name == Plugin.files:
        from .files import plugin_info as _files_plugin_info
        return _files_plugin_info
    raise NotImplementedError(f"Unsupported plugin: {name}")


def get_plugin_backup_class(name):
    return _get_plugin_info(name)["backup"]


def get_plugin_restore_class(name):
    return _get_plugin_info(name)["restore"]


def get_plugin_config_class(name):
    return _get_plugin_info(name)["config"]


def get_plugin_manifest_class(name):
    return _get_plugin_info(name)["manifest"]

from .files import plugin_info as _files_plugin_info
from astacus.common.ipc import Plugin


def _get_plugin_info(name):
    return {Plugin.files: _files_plugin_info}[name]


def get_plugin_backup_class(name):
    return _get_plugin_info(name)["backup"]


def get_plugin_restore_class(name):
    return _get_plugin_info(name)["restore"]


def get_plugin_config_class(name):
    return _get_plugin_info(name)["config"]


# TBD: Not yet used by the only existing 'files' backend, but this
# will be (json encoded) nested dict class stored within backup
# manifest for e.g. m3 purposes.
def get_plugin_data_class(name):
    return _get_plugin_info(name)["data"]

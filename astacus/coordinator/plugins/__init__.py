from .files import plugin_info as _files_plugin_info
from .m3 import plugin_info as _m3_plugin_info
from astacus.common.ipc import Plugin


def _get_plugin_info(name):
    return {Plugin.files: _files_plugin_info, Plugin.m3: _m3_plugin_info}[name]


def get_plugin_backup_class(name):
    return _get_plugin_info(name)["backup"]


def get_plugin_restore_class(name):
    return _get_plugin_info(name)["restore"]


def get_plugin_config_class(name):
    return _get_plugin_info(name)["config"]


def get_plugin_manifest_class(name):
    return _get_plugin_info(name)["manifest"]

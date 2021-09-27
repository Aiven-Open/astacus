from .files import FilesPlugin
from .m3db import M3DBPlugin
from astacus.common.ipc import Plugin

PLUGINS = {
    Plugin.files: FilesPlugin,
    Plugin.m3db: M3DBPlugin,
}

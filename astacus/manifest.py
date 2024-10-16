"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details

Manifest dump utility. Allows to examine the contents of the object storage
without a running Astacus process.

"""

from astacus.common.rohmustorage import RohmuConfig, RohmuStorage

import json


def create_manifest_parsers(parser, subparsers):
    p_manifest = subparsers.add_parser("manifest", help="Examine Astacus backup manifests")
    p_manifest.add_argument("-c", "--config", type=str, required=True, help="Astacus server configuration file to use")
    p_manifest.add_argument("-s", "--storage", type=str, help="Storage to use")
    manifest_subparsers = p_manifest.add_subparsers(title="Manifest commands")
    create_list_parser(manifest_subparsers)


def create_list_parser(subparsers):
    p_list = subparsers.add_parser("list", help="List backup manifests in object storage")
    p_list.set_defaults(func=_run_list)


def _run_list(args):
    rohmu_storage = _create_rohmu_storage(args.config, args.storage)
    json_names = rohmu_storage.list_jsons()
    for _json in json_names:
        print(_json)


def _create_rohmu_storage(config_path: str, storage: str) -> RohmuStorage:
    with open(config_path, "r") as config_fp:
        config_json = json.load(config_fp)
    if "object_storage" not in config_json:
        raise ValueError(f"object_storage key missing in {config_path}")
    rohmu_config = RohmuConfig.parse_obj(config_json["object_storage"])
    return RohmuStorage(rohmu_config, storage=storage)

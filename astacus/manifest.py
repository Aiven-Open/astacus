"""Copyright (c) 2024 Aiven Ltd
See LICENSE for details.

Manifest dump utility. Allows to examine the contents of the object storage
without a running Astacus process.

"""

from argparse import ArgumentParser
from astacus.common import ipc
from astacus.common.rohmustorage import RohmuConfig, RohmuStorage
from pathlib import Path

import base64
import json
import logging
import msgspec
import shutil
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_manifest_parsers(parser: ArgumentParser, subparsers):
    p_manifest = subparsers.add_parser("manifest", help="Examine Astacus backup manifests")
    p_manifest.add_argument("-c", "--config", type=str, required=True, help="Astacus server configuration file to use")
    p_manifest.add_argument("-s", "--storage", type=str, help="Storage to use")
    manifest_subparsers = p_manifest.add_subparsers(title="Manifest commands")
    create_list_parser(manifest_subparsers)
    create_describe_parser(manifest_subparsers)
    create_dump_parser(manifest_subparsers)
    create_download_files_parser(manifest_subparsers)


def create_list_parser(subparsers):
    p_list = subparsers.add_parser("list", help="List backup manifests in object storage")
    p_list.set_defaults(func=_run_list)


def create_describe_parser(subparsers):
    p_describe = subparsers.add_parser("describe", help="Print info/statistics from a backup manifest")
    p_describe.add_argument("manifest", type=str, help="Manifest object name (can be obtained by running manifest list)")
    p_describe.set_defaults(func=_run_describe)


def create_dump_parser(subparsers):
    p_dump = subparsers.add_parser("dump", help="Dump contents of the manifest to standard output")
    p_dump.add_argument(
        "-p", "--pretty", action="store_true", default=False, help="Parse JSON and apply indent/files/hashes"
    )
    p_dump.add_argument("-i", "--indent", type=int, help="How many spaces to use per tab in json output")
    p_dump.add_argument("-F", "--files", action="store_true", default=False, help="Dump files in snapshot results")
    p_dump.add_argument("-H", "--hashes", action="store_true", default=False, help="Dump hashes in snapshot results")
    p_dump.add_argument("manifest", type=str, help="Manifest object name (can be obtained by running manifest list)")
    p_dump.set_defaults(func=_run_dump)


def create_download_files_parser(subparsers):
    p_download_files = subparsers.add_parser("download-files", help="Download files from a backup manifest")
    p_download_files.add_argument(
        "manifest", type=str, help="Manifest object name (can be obtained by running manifest list)"
    )
    p_download_files.add_argument("destination", type=str, help="Destination directory to download files to")
    p_download_files.add_argument("--prefix", type=str, help="Prefix to filter files", required=True)
    p_download_files.add_argument("--node", type=int, help="Node index to download files from", required=True)
    p_download_files.set_defaults(func=_run_download_files)


def _run_download_files(args):
    rohmu_storage = _create_rohmu_storage(args.config, args.storage)
    manifest = rohmu_storage.download_json(args.manifest, ipc.BackupManifest)
    destination = Path(args.destination)
    if args.node >= len(manifest.upload_results):
        raise ValueError(f"Node {args.node} not found in manifest")
    snapshot_result = manifest.snapshot_results[args.node]
    assert snapshot_result.state
    for snapshot_file in snapshot_result.state.files:
        if not snapshot_file.relative_path.startswith(args.prefix):
            continue

        path = destination / snapshot_file.relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading %s to %s", snapshot_file.relative_path, path)
        if snapshot_file.hexdigest:
            rohmu_storage.download_hexdigest_to_path(snapshot_file.hexdigest, path)
        else:
            assert snapshot_file.content_b64 is not None
            path.write_bytes(base64.b64decode(snapshot_file.content_b64))


def _run_list(args):
    rohmu_storage = _create_rohmu_storage(args.config, args.storage)
    json_names = rohmu_storage.list_jsons()
    for _json in json_names:
        print(_json)


def _run_describe(args):
    rohmu_storage = _create_rohmu_storage(args.config, args.storage)
    manifest = rohmu_storage.download_json(args.manifest, ipc.BackupManifest)
    print(manifest.plugin, "manifest", args.manifest)
    print("===============================")
    print("Started at:", manifest.start.isoformat())
    print("Finished at:", manifest.end.isoformat())
    print("Attempts:", manifest.attempt)
    for i, snapshot_result in enumerate(manifest.snapshot_results):
        print(
            f"Snapshot #{i} created at {snapshot_result.start.isoformat()}",
            f"contains {snapshot_result.files} files ({snapshot_result.total_size} bytes)",
        )
    for i, upload_result in enumerate(manifest.upload_results):
        print(f"Node #{i} uploaded {upload_result.total_stored_size} bytes out of {upload_result.total_size}")
    if manifest.tiered_storage_results:
        print(
            f"References {manifest.tiered_storage_results.n_objects} objects in tiered storage",
            f"totalling {manifest.tiered_storage_results.total_size_bytes} bytes",
        )


def _run_dump(args):
    rohmu_storage = _create_rohmu_storage(args.config, args.storage)
    with rohmu_storage.open_json_bytes(args.manifest) as mapped_file:
        if args.pretty:
            _dump_manifest_pretty(msgspec.json.decode(mapped_file), args)
        else:
            _dump_manifest_raw(mapped_file)
        print()


def _dump_manifest_pretty(manifest_json, args):
    if not args.files:
        _redact_snapshot_files(manifest_json)
    if not args.hashes:
        _redact_snapshot_hashes(manifest_json)
    json.dump(manifest_json, sys.stdout, indent=args.indent or 2)


def _dump_manifest_raw(mapped_file):
    shutil.copyfileobj(mapped_file, sys.stdout.buffer)
    sys.stdout.buffer.flush()


def _create_rohmu_storage(config_path: str, storage: str) -> RohmuStorage:
    with open(config_path) as config_fp:
        config_json = json.load(config_fp)
    if "object_storage" not in config_json:
        raise ValueError(f"object_storage key missing in {config_path}")
    rohmu_config = RohmuConfig.parse_obj(config_json["object_storage"])
    return RohmuStorage(rohmu_config, storage=storage)


def _redact_snapshot_files(manifest_json):
    for snapshot_result in manifest_json.get("snapshot_results", []):
        if "state" not in snapshot_result or "files" not in snapshot_result["state"]:
            continue
        num_files = len(snapshot_result["state"]["files"])
        snapshot_result["state"]["files"] = f"<redacted, has {num_files} entries>"


def _redact_snapshot_hashes(manifest_json):
    for snapshot_result in manifest_json.get("snapshot_results", []):
        if "hashes" not in snapshot_result:
            continue
        num_hashes = len(snapshot_result["hashes"])
        snapshot_result["hashes"] = f"<redacted, has {num_hashes} entries>"

"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.

Client commands for Astacus tool

"""

from astacus.common import ipc, magic, utils
from astacus.common.utils import exponential_backoff, http_request
from tabulate import tabulate

import json as _json
import logging
import msgspec
import time

logger = logging.getLogger(__name__)


def _run_op(op, args, *, json=None, data=None) -> bool:
    print(f"Starting {op}..")
    start = time.monotonic()
    if json is not None:
        data = _json.dumps(json)
    r = http_request(f"{args.url}/{op}", method="post", caller="client._run_op", data=data)
    if r is None:
        return False
    if not args.wait_completion:
        elapsed = time.monotonic() - start
        print(f".. which took {elapsed} seconds")
        return True
    url = r["status_url"]
    for _ in exponential_backoff(initial=0.1, duration=args.wait_completion):
        logger.debug("Checking {op} status at %r", url)
        r = http_request(url, caller="client._run_op[2]")
        if r is None:
            return False
        state = r["state"]
        if state == "fail":
            print(".. which eventually failed")
            return False
        assert state != "fail"
        if state == "done":
            break
    else:
        print(".. which timed out")
        return False
    elapsed = time.monotonic() - start
    print(f".. which took {elapsed} seconds")
    return True


def _run_restore(args) -> bool:
    json = {}
    if args.storage:
        json["storage"] = args.storage
    if args.backup:
        json["name"] = args.backup
    if args.stop_after_step:
        json["stop_after_step"] = args.stop_after_step
    return _run_op("restore", args, json=json)


def print_list_result(result):
    for i, storage in enumerate(result.storages):
        gheaders = {
            "storage": "Storage",
            "plugin": "Plugin",
            "pchanged": "% change",
            "csaving": "Comp %",
            "attempt": "Att",
            "nodes": "Nodes",
        }
        headers = {
            "plugin": "Plugin",
            "name": "Name",
            # "start": "Start", n/a, name =~ same
            "attempt": "Att",
            "duration": "Duration",
            "nodes": "Nodes",
            "files": "Files",
            "total_size": "Size",
        }
        # These are fields which are in both gheaders and headers,
        # and they should be retained and populated
        # - in neither if no values set
        # - in global headers if all values are same
        # - in table rows if 2+ distinct values are present
        same_or_global = ["plugin", "attempt", "nodes"]
        gtable = [{"storage": storage.storage_name}]
        table = []
        tsize, usize, ssize = 0, 0, 0
        local_fields = []
        for field_name in same_or_global:
            assert field_name in gheaders
            assert field_name in headers
            values = list(set(getattr(b, field_name) for b in storage.backups))
            if len(values) < 1:
                del gheaders[field_name]
                del headers[field_name]
            elif len(values) == 1:
                del headers[field_name]
                gtable[0][field_name] = values[0]
            else:
                local_fields.append(field_name)
                del gheaders[field_name]

        for b in storage.backups:
            table.append(
                {
                    "name": b.name,
                    # "start": b.start,
                    "duration": utils.timedelta_as_short_str(b.end - b.start),
                    "files": b.files,
                    "total_size": utils.size_as_short_str(b.total_size),
                }
            )
            for field_name in local_fields:
                table[-1][field_name] = getattr(b, field_name)
            tsize += b.total_size
            usize += b.upload_size
            ssize += b.upload_stored_size

        if tsize:
            pchanged = 100.0 * usize / tsize
            gtable[0]["pchanged"] = f"{pchanged:.2f}%"
        if usize:
            csaving = 100.0 - 100.0 * ssize / usize
            gtable[0]["csaving"] = f"{csaving:.2f}%"

        if i:
            print()
        print(tabulate(gtable, headers=gheaders, tablefmt="github"))
        print()
        print(tabulate(table, headers=headers, tablefmt="github"))


def _run_list(args, list_path: str) -> bool:
    storage_name = ""
    if args.storage:
        storage_name = f"?storage={args.storage}"
    r = http_request(f"{args.url}/{list_path}{storage_name}", caller="client._run_list")
    if r is None:
        return False
    print_list_result(msgspec.convert(r, ipc.ListResponse))
    return True


def _run_cleanup(args) -> bool:
    json = {}  # type: ignore
    # Copy retention fields from argparser arguments to the json request, if set
    for field in msgspec.structs.fields(ipc.Retention):
        v = getattr(args, field.encode_name, None)
        if v:
            json.setdefault("retention", {})[field.encode_name] = v
    return _run_op("cleanup", args, json=json)


def _run_delete(args) -> bool:
    return _run_op("cleanup", args, json={"explicit_delete": args.backups})


def _reload_config(args) -> bool:
    start_time = time.monotonic()
    for _ in exponential_backoff(initial=0.1, duration=args.wait_completion):
        # Wait as much as we can, without going over the global wait_completion limit
        timeout = args.wait_completion - (time.monotonic() - start_time)
        response = http_request(f"{args.url}/config/reload", method="post", caller="client._reload_config", timeout=timeout)
        if response is not None:
            return True
    return False


def _check_reload_config(args) -> bool:
    response = http_request(f"{args.url}/config/status", method="get", caller="client._check_reload_config")
    if response is None:
        print("Failed to get configuration status")
        return False
    if args.status:
        return not response["needs_reload"]
    if response["needs_reload"]:
        print("Configuration needs to be reloaded")
    else:
        print("Configuration does not need to be reloaded")
    return True


def _add_backup(p_parent, url_path: str) -> None:
    p_backup = p_parent.add_parser("backup", help="Request backup")
    p_backup.set_defaults(func=lambda args: _run_op(url_path, args))


def _add_list(p_parent, url_path: str) -> None:
    p_delta_list = p_parent.add_parser("list", help="List backups")
    p_delta_list.add_argument("--storage", help="Particular storage to list (default: all)")
    p_delta_list.set_defaults(func=lambda args: _run_list(args, url_path))


def create_client_parsers(parser, subparsers):
    default_url = f"http://localhost:{magic.ASTACUS_DEFAULT_PORT}"
    parser.add_argument("-u", "--url", type=str, help="Astacus REST endpoint URL", default=default_url)
    parser.add_argument(
        "-w",
        "--wait-completion",
        type=int,
        help="Wait at most this long the requested (client) operation to complete (unit:seconds)",
    )

    p_reload = subparsers.add_parser("reload", help="Reload astacus configuration")
    p_reload.set_defaults(func=_reload_config)

    p_check_reload = subparsers.add_parser(
        "check-reload", help="Check if the astacus configuration needs to be reloaded from disk"
    )
    p_check_reload.set_defaults(func=_check_reload_config)
    p_check_reload.add_argument(
        "--status", action="store_true", help="Returns a status code of 1 if the configuration needs to be reloaded"
    )

    _add_backup(subparsers, "backup")

    p_restore = subparsers.add_parser("restore", help="Request backup restoration")
    p_restore.add_argument("--storage", help="Storage to use (default: configured)")
    p_restore.add_argument("--backup", help="Name of backup to restore (default: most recent)")
    p_restore.add_argument("--stop-after-step", type=str, help="Stop restore operation after the specified step")
    p_restore.set_defaults(func=_run_restore)

    _add_list(subparsers, "list")

    p_cleanup = subparsers.add_parser("cleanup", help="Delete backups that should no longer be kept")
    p_cleanup.add_argument("--minimum-backups", type=int, help="Minimum number of backups to be kept")
    p_cleanup.add_argument("--maximum-backups", type=int, help="Maximum number of backups to be kept")
    p_cleanup.add_argument(
        "--keep-days", type=int, help="Number of days to keep backups (does not override minimum/maximum-backups)"
    )

    p_cleanup.set_defaults(func=_run_cleanup)

    p_delete = subparsers.add_parser("delete", help="Delete specific old existing backup(s)")
    p_delete.add_argument("--backups", nargs="+", help="Backup names to be deleted")
    p_delete.set_defaults(func=_run_delete)

    p_delta = subparsers.add_parser("delta", help="Delta backup commands")
    sub_delta = p_delta.add_subparsers()
    _add_backup(sub_delta, "delta/backup")
    _add_list(sub_delta, "delta/list")

"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Client commands for Astacus tool

"""

from astacus.common import ipc, magic, utils
from astacus.common.utils import exponential_backoff, http_request
from tabulate import tabulate

import json as _json
import logging
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


def _run_backup(args) -> bool:
    return _run_op("backup", args)


def _run_restore(args) -> bool:
    json = {}
    if args.storage:
        json["storage"] = args.storage
    if args.backup:
        json["name"] = args.backup
    return _run_op("restore", args, json=json)


def print_list_result(result):
    for i, storage in enumerate(result.storages):
        gheaders = {
            "storage": "Storage",
            "plugin": "Plugin",
            "pchanged": "% change",
            "csaving": "Comp %",
            "attempt": "Att",
            "nodes": "Nodes"
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
            table.append({
                "name": b.name,
                # "start": b.start,
                "duration": utils.timedelta_as_short_str(b.end - b.start),
                "files": b.files,
                "total_size": utils.size_as_short_str(b.total_size),
            })
            for field_name in local_fields:
                table[-1][field_name] = getattr(b, field_name)
            tsize += b.total_size
            usize += b.upload_size
            ssize += b.upload_stored_size

        if tsize:
            gtable[0]["pchanged"] = "%.2f%%" % (100.0 * usize / tsize)
        if usize:
            gtable[0]["csaving"] = "%.2f%%" % (100.0 - 100.0 * ssize / usize)

        # headers type hint is for some reason wrong - it accepts Dict[str,str]
        if i:
            print()
        print(tabulate(gtable, headers=gheaders, tablefmt="github"))  # type: ignore
        print()
        print(tabulate(table, headers=headers, tablefmt="github"))  # type: ignore


def _run_list(args) -> bool:
    storage_name = ""
    if args.storage:
        storage_name = f"?storage={args.storage}"
    r = http_request(f"{args.url}/list{storage_name}", caller="client._run_list")
    if r is None:
        return False
    print_list_result(ipc.ListResponse.parse_obj(r))
    return True


def _run_cleanup(args) -> bool:
    json = {}  # type: ignore
    # Copy retention fields from argparser arguments to the json request, if set
    for k in ipc.Retention().dict().keys():
        v = getattr(args, k, None)
        if v:
            json.setdefault("retention", {})[k] = v
    return _run_op("cleanup", args, json=json)


def _run_delete(args) -> bool:
    return _run_op("cleanup", args, json={"explicit_delete": args.backups})


def create_client_parsers(parser, subparsers):
    default_url = f"http://localhost:{magic.ASTACUS_DEFAULT_PORT}"
    parser.add_argument("-u", "--url", type=str, help="Astacus REST endpoint URL", default=default_url)
    parser.add_argument(
        "-w",
        "--wait-completion",
        type=int,
        help="Wait at most this long the requested (client) operation to complete (unit:seconds)"
    )

    p_backup = subparsers.add_parser("backup", help="Request backup")
    p_backup.set_defaults(func=_run_backup)

    p_restore = subparsers.add_parser("restore", help="Request backup restoration")
    p_restore.add_argument("--storage", help="Storage to use (default: configured)")
    p_restore.add_argument("--backup", help="Name of backup to restore (default: most recent)")
    p_restore.set_defaults(func=_run_restore)

    p_list = subparsers.add_parser("list", help="List backups")
    p_list.add_argument("--storage", help="Particular storage to list (default: all)")
    p_list.set_defaults(func=_run_list)

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

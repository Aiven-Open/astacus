"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Client commands for Astacus tool

"""

from astacus.common import ipc, magic, utils
from astacus.common.utils import exponential_backoff, http_request
from tabulate import tabulate

import logging
import time

logger = logging.getLogger(__name__)


def _run_op(op, args) -> bool:
    print(f"Starting {op}..")
    start = time.monotonic()
    r = http_request(f"{args.url}/{op}", method="post", caller="client._run_op")
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
    return _run_op("restore", args)


def _run_list(args) -> bool:
    storage_name = ""
    if args.storage:
        storage_name = f"?storage={args.storage}"
    r = http_request(f"{args.url}/list{storage_name}", caller="client._run_list")
    if r is None:
        return False
    result = ipc.ListResponse.parse_obj(r)
    for i, storage in enumerate(result.storages):
        gheaders = {"storage": "Storage"}
        gtable = [{"storage": storage.storage_name}]
        plugin_same = len(set(b.plugin for b in storage.backups)) <= 1
        headers = {
            "name": "Name",
            # "start": "Start", n/a, name =~ same
            "attempt": "Attempt",
            "duration": "Duration",
            "files": "Files",
            "total_size": "Size",
        }
        table = [
            {
                "plugin": b.plugin,
                "name": b.name,
                "attempt": b.attempt,
                # "start": b.start,
                "duration": utils.timedelta_as_short_str(b.end - b.start),
                "files": b.files,
                "total_size": b.total_size
            } for b in storage.backups
        ]
        if not plugin_same:
            headers["plugin"] = "Plugin"
        else:
            # delete the plugin fields from table
            for e in table:
                del e["plugin"]
            if storage.backups:
                gheaders["plugin"] = "Plugin"
                gtable[0]["plugin"] = storage.backups[0].plugin

        # headers type hint is for some reason wrong - it accepts Dict[str,str]
        if i:
            print()
        print(tabulate(gtable, headers=gheaders, tablefmt="github"))  # type: ignore
        print()
        print(tabulate(table, headers=headers, tablefmt="github"))  # type: ignore

    return True


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
    p_restore.set_defaults(func=_run_restore)

    p_list = subparsers.add_parser("list", help="List backups")
    p_list.add_argument("--storage", help="Particular storage to list (default: all)")
    p_list.set_defaults(func=_run_list)

"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Client commands for Astacus tool

"""

from astacus.common import magic
from astacus.common.utils import exponential_backoff

import logging
import requests
import time

logger = logging.getLogger(__name__)


def _run_op(op, args) -> bool:
    print(f"Starting {op}..")
    start = time.monotonic()
    r = None
    try:
        r = requests.post(f"{args.url}/{op}")
    except Exception as ex:  # pylint: disable=broad-except
        print(f"Unable to connect Astacus at {args.url}: {ex!r}")
        return False
    if not r.ok:
        print(f"Astacus not happy: {r!r}")
        return False
    if not args.wait_completion:
        elapsed = time.monotonic() - start
        print(f".. which took {elapsed} seconds")
        return True
    url = r.json()["status_url"]
    for _ in exponential_backoff(initial=0.1, duration=args.wait_completion):
        logger.debug("Checking {op} status at %r", url)
        r = requests.get(url)
        assert r and r.ok
        state = r.json()["state"]
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


def create_client_parsers(parser, subparsers):
    default_url = f"http://localhost:{magic.ASTACUS_DEFAULT_PORT}"
    parser.add_argument("-u", "--url", type=str, help="Astacus REST endpoint URL", default=default_url)
    parser.add_argument(
        "-w",
        "--wait-completion",
        type=int,
        help="Wait at most this long the requested (client) operation to complete (unit:seconds)"
    )

    backup = subparsers.add_parser("backup", help="Request backup")
    backup.set_defaults(func=_run_backup)

    restore = subparsers.add_parser("restore", help="Request backup restoration")
    restore.set_defaults(func=_run_restore)

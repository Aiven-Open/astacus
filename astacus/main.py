"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Main module for astacus.

"""

from astacus import client, manifest, server

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(description="Astacus - cluster backup tool")
    subparsers = parser.add_subparsers(title="Commands")
    server.create_server_parser(subparsers)
    client.create_client_parsers(parser, subparsers)
    manifest.create_manifest_parsers(parser, subparsers)
    args = parser.parse_args()
    try:
        func = args.func
    except AttributeError:
        parser.print_help(sys.stderr)
        sys.exit(1)
    success = func(args)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()

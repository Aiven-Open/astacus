#!/usr/bin/env bash

set -euxo pipefail

rm bench/memory.* || true
rm bench/sqlite.* || true

rm -rf /tmp/snapshot_dst/*

mprof run -o bench/memory.dat --include-children python -m bench.bench_snapshotter memory "$1" /tmp/snapshot_dst 5
mprof plot bench/memory.dat -o bench/memory.png

rm -rf /tmp/snapshot_dst/*

mprof run -o bench/sqlite.dat --include-children python -m bench.bench_snapshotter sqlite "$1" /tmp/snapshot_dst 5
mprof plot bench/sqlite.dat -o bench/sqlite.png

open bench/*.png

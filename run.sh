#!/usr/bin/env bash
# usage: run.sh LOG cmd...
log=$1; shift
S=/home/paudley/stage/scripts/cargo
{
echo "pwd=$PWD"; echo "HEAD=$(git rev-parse HEAD)"; echo "cargo: $(stat -c %y $S) $(sha256sum $S)"
"$@"; echo "EXIT=$?"
echo "cargo-end: $(stat -c %y $S) $(sha256sum $S)"; echo "HEAD-end=$(git rev-parse HEAD)"
} > "$log" 2>&1

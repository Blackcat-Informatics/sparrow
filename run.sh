#!/bin/bash
# usage: run.sh LOG cmd...
log=$1; shift
W=/home/paudley/stage/scripts/cargo
{
pwd; git rev-parse HEAD; stat -c '%y' $W; sha256sum $W
"$@"
echo EXIT=$?
pwd; git rev-parse HEAD; stat -c '%y' $W; sha256sum $W
} > "$log" 2>&1

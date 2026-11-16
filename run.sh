#!/bin/bash
# usage: run.sh LOGNAME cmd...
cd /home/paudley/Active/sparrow/.worktrees/327-sparrow-archive-extract-take-one || exit 9
log=.stage/sparrow-archive-extract-take-one/logs/$1.log
shift
{
pwd; git rev-parse HEAD; stat -c '%y' /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo
"$@"
echo EXIT=$?
pwd; git rev-parse HEAD; stat -c '%y' /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo
} > "$log" 2>&1
tail -n 40 "$log"

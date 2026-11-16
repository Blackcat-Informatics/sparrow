#!/bin/bash
# usage: run.sh logname cmd...
cd /home/paudley/Active/sparrow/.worktrees/319-archive-prepare-holds-the-single-rdf || exit 1
log=.stage/archive-prepare-holds-the-single-rdf/logs/$1.log; shift
hdr() { pwd; git rev-parse HEAD; stat -c '%y' /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo; }
{ hdr; "$@"; echo EXIT=$?; hdr; } > "$log" 2>&1

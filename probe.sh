#!/bin/bash
cd /home/paudley/Active/sparrow/.worktrees/319-archive-prepare-holds-the-single-rdf || exit 1
log=.stage/archive-prepare-holds-the-single-rdf/logs/probe319-$1.log
hdr() { pwd; git rev-parse HEAD; stat -c '%y' /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo; }
{ hdr; env N=50000 $2 cargo test -p sparrow-core --release --test probe319 -- --nocapture; echo EXIT=$?; hdr; } > "$log" 2>&1

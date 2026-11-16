#!/bin/bash
# usage: run.sh LOG cmd...
log=$1; shift
cd /home/paudley/Active/sparrow/.worktrees/340-ingest-refuses-a-json-artifact-with-rdf
{
echo "pwd=$PWD"; echo "HEAD=$(git rev-parse HEAD)"
stat -c %y /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo
flock /home/paudley/Active/sparrow/.stage/cargo.lock "$@"
echo "EXIT=$?"
stat -c %y /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo
} > "$log" 2>&1

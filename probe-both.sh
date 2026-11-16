#!/usr/bin/env bash
# Runs the temporary request probe for the log shape (PROBE_RECORDS) and one 800-key JSON record.
set -u
cargo test -p sparrow-core --release --lib probe_328 -- --nocapture
a=$?
PROBE_SHAPE=json800 cargo test -p sparrow-core --release --lib probe_328 -- --nocapture
b=$?
echo "log_shape_exit=$a json800_exit=$b"
[[ $a == 0 && $b == 0 ]]

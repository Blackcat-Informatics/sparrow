#!/bin/bash
set -e
f=payload_index.rs
n=$(grep -n '^/// The counters. bound for this migration' $f | cut -d: -f1)
head -n $((n-1)) $f > $f.new
mv $f.new $f
tail -5 $f

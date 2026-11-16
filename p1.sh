#!/bin/bash
set -e
perl -0pi -e 's/    while store.migrations\(\).unwrap\(\)\[1\].watermark.is_some\(\) \{\n[^\n]*\n        break;\n    \}\n//' probe_payload_index.rs

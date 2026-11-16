#!/bin/bash
set -e
f=migration.rs
perl -0pi -e 's/        store_counters::fence\(db, &mut batch\)\?;\n        batch.put\(\n            "store_format",\n            writer_format::enable_store_counters\(&format\)\?,\n        \);\n    \}\n/        store_counters::fence(db, &mut batch)?;\n        format = writer_format::enable_store_counters(&format)?.to_vec();\n        batch.put("store_format", &format);\n    }\n    \/\/ After the counters, whose zero rows it extends with its own.\n    if !writer_format::has_payload_index(&format) {\n        payload_index::fence(db, &format, &mut batch)?;\n    }\n/' $f

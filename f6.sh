#!/bin/bash
set -e
f=payload_index.rs
perl -0pi -e 's/fn config\(\) -> Config \{\n    sparrow_test_support::storage_config!\(Config\)\n\}\n\n//' $f
perl -0pi -e 's/fn payload_id\(bytes: &\[u8\]\) -> String \{\n[^\n]*\n[^\n]*\n\}\n\n//' $f
sed -i 's/config())/sparrow_test_support::storage_config!(Config))/' $f
perl -0pi -e 's/fn held\(store: &Store, bytes: &\[u8\]\)/fn held(store: &Store, id: &str)/; s/    let id = payload_id\(bytes\);\n//; s/store.payload_lookup\(&id, after/store.payload_lookup(id, after/' $f
sed -i 's/held(&store, b"same\\n")/held(\&store, \&first.payload_id)/; s/held(&store, b"nothing holds this\\n")/held(\&store, \&format!("blake3:{}", "0".repeat(64)))/; s/held(&store, b"retired\\n")/held(\&store, \&evicted.payload_id)/; s/held(&store, b"shared\\n")/held(\&store, \&old.payload_id)/; s/held(&store, b"")/held(\&store, \&empty.payload_id)/; s/held(&store, b"old\\n")/held(\&store, \&before.payload_id)/; s/held(&store, b"new\\n")/held(\&store, \&during_payload)/' $f
sed -i 's/\.payload_lookup(&payload_id(b"old\\n"), None, 10)/.payload_lookup(\&before.payload_id, None, 10)/; s/\.payload_lookup(&payload_id(b"new\\n"), None, 10)/.payload_lookup(\&during.payload_id, None, 10)/' $f
sed -i 's/during_payload/during.payload_id/' $f

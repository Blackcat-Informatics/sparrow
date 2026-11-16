#!/bin/bash
set -e
perl -0pi -e 's/(    let step = \|store: &Store\| \{\n        store.migrate_step\(\).unwrap\(\);\n    \};\n)(\n    let read = \|cursor: Option<String>, now\| \{)/$1    \/\/ The counters seed one prefix more since the payload index (#323): one more page first.\n    step(&store);\n$2/' storage.rs
perl -0pi -e 's/    assert_eq!\(page.migration, sparrow_core::Migration::OwnerGraphIndex\);\n    assert!\(page.complete\);\n    drop\(store\);/    while page.migration == sparrow_core::Migration::PayloadIndex {\n        page = store.migrate_step().unwrap().unwrap();\n    }\n    assert_eq!(page.migration, sparrow_core::Migration::OwnerGraphIndex);\n    assert!(page.complete);\n    drop(store);/' storage.rs

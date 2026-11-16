#!/bin/bash
set -e
perl -0pi -e 's/    \/\/ Expiry deletes the late artifact.s entry thirty days and a day after receipt.\n/    \/\/ Expiry retires the late artifact\x27s chunks at its deadline, a day before it deletes the\n    \/\/ entry: its bytes are released, so the index must already not list it.\n    store.expire(40 * DAY + RETENTION_MS).unwrap();\n    assert_eq!(held(&store, &old.payload_id), (vec![], true));\n    counted(&store, 0);\n    \/\/ A day later expiry deletes the entry itself.\n/' payload_index.rs
sed -i 's/^\/\/\/ INVARIANT (#323): the key leaves the index in the batch that ends the artifact.s$/&/' payload_index.rs

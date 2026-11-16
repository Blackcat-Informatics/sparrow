#!/bin/bash
set -e
perl -0pi -e 's/            "source-summary",\n            "source-context"\n        \]/            "source-summary",\n            "source-context",\n            "payload-index"\n        ]/' sparrow-core/tests/storage.rs
perl -0pi -e 's/    assert!\(pending\[2\.\.\]\.iter\(\)\.all\(\|m\| m\.complete\(\)\), "\{pending:\?\}"\);/    assert!(pending[2..4].iter().all(|m| m.complete()), "{pending:?}");/' sparrow-core/tests/storage.rs
perl -0pi -e 's/            Migration::SourceSummary,\n            Migration::OwnerGraphIndex,\n            Migration::SourceContext,\n        \]\n    \);\n    assert_eq!\(names\(&store\), report\);/            Migration::SourceSummary,\n            Migration::PayloadIndex,\n            Migration::OwnerGraphIndex,\n            Migration::SourceContext,\n        ]\n    );\n    assert_eq!(names(&store), report);/' sparrow-core/tests/storage.rs
sed -i 's/assert_eq!(migrations.len(), 4, "{migrations:?}");/assert_eq!(migrations.len(), 5, "{migrations:?}");/' sparrow-server/tests/rpc.rs
git diff --stat sparrow-core/tests sparrow-server/tests

#!/bin/bash
set -e
perl -0pi -e 's/`store-counters` first and `source-summary` second, since, until the source\nsummary completes, every source and live artifact listing first reads what that\nmigration still owes;/`store-counters` first and `source-summary` second, since, until the source\nsummary completes, every source and live artifact listing first reads what that\nmigration still owes, then `payload-index`;/' architecture.md
cat > par.txt <<'EOF'
Content lookup reads the payload id index. An artifact's `payload_id` is the BLAKE3
hash of all its own bytes, computed once in the ingest path, and the default family
holds one empty-valued key `payload-id:<64 hex digest>:<artifact id>` for each complete
artifact, so a digest's holders are one prefix scan and "do I already hold this
content" is a point read. One owner observes every write batch and adds the key
changes to it: an artifact entry is never created, retired (evicted through its
manifest retirement state) or deleted (expiry, recordless eviction) without its key
following in the same atomic batch, and an archived artifact stays indexed until
then. A store counter row counts the keys through the one batch observer, so a count
that disagrees with the complete artifacts is detectable. Tail checkpoints, whose
artifact is exactly the checkpointed range, recordless artifacts and published imports
all carry the digest of the whole artifact; the parts of a consume-root set are each
their own artifact and the digest of the whole source file is never indexed.
The `payload-index` online migration seeds the keys of an older store in bounded pages
with its checkpoint, as the other migrations do; until it completes, the lookup says
it is pending, so a digest it does not list is unknown, never absent.

EOF
n=$(grep -n '^Source and artifact discovery read three derived catalog row families' architecture.md | cut -d: -f1)
sed -i "$((n-1))r par.txt" architecture.md
rm par.txt

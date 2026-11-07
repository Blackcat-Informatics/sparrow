Refs #36

The open now applies the schema 69 writer-format fence and seeds both migration checkpoints, then serves at once. The owner graph index build and the event-fallback backfill run as a maintenance duty, one page per admission, each page committing its rows with its checkpoint, so a restart resumes. Readers say when a migration is pending, status reports each migration's progress and failure, and strict reads refuse until the backfill reaches their records.

`sparrowd --no-intake` runs a writable daemon that opens no stored watch root, and `deploy/verify-backup.sh --measure-migration` uses it to time a backup copy's first open and backfill under the unit's limits, with a disk guard.

Reviews 2 to 4 are in the stage notes; review 4 passed. The acceptance measurement on a nightly copy has not run: it needs 50 GB free on the root filesystem and the host is below that. The issue stays open until that measurement is in.

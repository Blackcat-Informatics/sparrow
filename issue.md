# Issue #327: sparrow-archive extract: take one artifact out of the GTS archive, verified; remove restore

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Operator's word (relayed by coordination, 2026-11-15 23:40): archive means automated age-out to the NAS in GTS files; restore means extracting a file from a GTS. There is no restore into Sparrow.

## Change
- Add `sparrow-archive extract DIR ARTIFACT_ID`: writes the artifact's bytes to standard output. It exits nonzero, and claims nothing (no bytes written to stdout), unless the BLAKE3 of the bytes equals the artifact's `payload_id`.
- An artifact whose records span two segments extracts whole.
- An id not found under DIR is a named error.
- Remove `sparrow_archive::restore` (`crates/sparrow-archive/src/restore.rs`, `RestoreReport`), its test `archived_segment_reimports_byte_exact`, and section 10's "Recovery is record by record" text and decision 6 in `docs/archive-format.md`. Nothing else calls `restore` (checked by grep).
- One short paragraph, in the format document and the tool's usage line, that a person can follow with only the NAS directory and the binary.

## Acceptance
- Tests: extract of a one-segment artifact and of an artifact spanning two segments equal the ingested bytes; a damaged segment or a wrong digest exits nonzero with nothing on stdout; an unknown id is a named error.
- The extraction of a partially present artifact (some records in a segment not under DIR) is refused by name, never partial.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only

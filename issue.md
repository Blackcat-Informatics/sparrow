# Issue #15: Encode archive segments in PurRDF Parquet with deterministic identities and quarantine

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Summary

Build the store-side archive segment encoder. It selects the next due range of the eviction order, gathers that range's bytes and graphs from a snapshot, and encodes them with `purrdf-columnar` into the five Parquet tables plus an N-Quads manifest. Every segment gets a deterministic identity, and entries that cannot be encoded are quarantined. The deletion gate already on `main` keeps every aged-out record until a segment covers it. This encoder is the first half of advancing that gate, so the 30-day window can turn over into a semi-permanent NAS archive instead of stopping.

## Current behavior

Verified on `main` at `ac6277f`.

- `crates/sparrow-core/src/storage/archive.rs` holds the deletion gate, the watermark and the fence.
  - Constants (`:20-27`): `ARCHIVE_START_FRACTION = 0.75`, `ARCHIVE_SEGMENT_BYTES = 64 MiB`, `ARCHIVE_MAX_SEGMENT_BYTES = 192 MiB` ("below the columnar codec's 256 MiB table bound") and `ARCHIVE_SEGMENT_RECORDS = 50_000`.
  - `ArchivePolicy::validate` (`:50-75`) admits `start_fraction` 0.5..0.95, segment bytes from `max_artifact_bytes` to 192 MiB, and at least one record.
  - `Store::archive_intend` (`:376-405`) durably records an `ArchiveIntent` (segment ID, first and last eviction positions, record and recordless-artifact counts) after `archive_range` (`:311-358`) checks that the range starts at the first unarchived entry, fits `max_segment_records` (`RESOURCE_LIMIT`) and matches the catalog (`CONFLICT`).
  - `Store::archive_commit` (`:409-432`) advances the watermark.
  - Nothing produces an intent: no code computes what is due, reads the range or encodes it.
- `docs/archive-format.md` specifies the format: layout (section 1), segment limits (section 2), identity (section 3), content (section 4), schedule (section 5), quarantine (section 6) and manifest (section 9). It marks P2 as not implemented and its memory figure as "an estimate to measure in P2".
- `crates/sparrow-core/src/storage/rdf_view.rs:894-899`: the public RDF view refuses logically expired evidence ("RDF selection contains expired evidence"). The encoder must read evidence that is past its ordinary deadline but held by the gate.
- `crates/sparrow-core/src/storage/eviction.rs:424`: `eviction_entries` walks the merged `time:` and `empty-time:` order that segments cover.
- PurRDF at the pinned revision `a47940e1` exposes `purrdf::columnar::{write, read, project}` through the facade (`crates/purrdf/src/lib.rs:150-151` in PurRDF). `write` takes any `DatasetView` and a `ContentStore`.

## Work

**Schedule (`crates/sparrow-core/src/storage/archive/schedule.rs`).** Implement `docs/archive-format.md` section 5 exactly.
- **By age:** an entry is due once `now >= received_at + start_fraction * (expires_at - received_at)`.
- **By size:** everything is due, oldest first, once `linked_physical_bytes >= start_fraction * (disk_budget - maintenance_reserve)`, or while pressure maintenance is requested.
- **Backlog:** a newly fenced store archives every due entry, oldest first, before anything is retired.
- **Horizon:** no segment passes the earliest of `now - max(10 minutes, buffered_flush_interval)`, the receipt time of the oldest open import preparation, and the oldest unsynchronized buffered entry.

**Range and content (`crates/sparrow-core/src/storage/archive/encode.rs`).**
- Add `Store::archive_prepare(now) -> Result<Option<PreparedSegment>, Error>`. It takes the writer lock only to capture a snapshot and the watermark, then walks the snapshot without the lock under the `rdf_read` lease, backing off when the lease is busy.
- The range starts at the first entry after the watermark. It closes at the first of `max_segment_bytes` of record payload, `max_segment_records` entries, the end of the first entry's UTC day, and the horizon.
- Gather the content in section 4 of the format:
  - each record's bytes, joined from its chunks, into a PurRDF `ContentStore` keyed by SHA-256;
  - the record metadata graph with its provenance and tracing facts, and the diagnostic graph;
  - the artifact graph, in the segment whose range contains the artifact's position;
  - the content projection graphs, from the store's single content-graph owner (after #5 that owner projects JSON from retained bytes);
  - one `sp:payloadSha256` link quad per record in `urn:sparrow:graph:archive-links`;
  - the reifiers and annotations of all of these.
- Read through a storage-internal archive scope that covers metadata and diagnostic graphs of entries whose payload is not retired, even past their ordinary deadline. It is not reachable from any RPC.
- Copy the view into an `RdfDatasetBuilder` through PurRDF's public interning seam, because PurRDF has no union view. File the upstream request for a union view and a streaming writer in PurRDF's tracker.

**Encoding.**
- Call `purrdf::columnar::write` with columnar schema 1 and Zstandard pages, unchanged. Add no local workarounds for its size or memory limits.
- Compute each table's BLAKE3, length and row count.
- Write the manifest with PurRDF's N-Quads serializer exactly as section 9 lists, with no wall-clock time.
- Compute the segment ID as section 3 defines, with `purrdf_hash::frame::frame_le`.
- A range whose largest table would pass the codec's bound is refused with `RESOURCE_LIMIT` before any intent is recorded.

**Quarantine.**
- An entry the codec refuses (`encode`), a record whose recorded projection failed (`projection`), and an entry whose bytes or projection were retired before the fence (`retired`) all leave the dataset.
- Its available raw bytes become quarantine file `<n>.bin`, with BLAKE3, SHA-256 and length in a `QuarantineEntry` in the manifest. The segment still covers its position.
- Encoding continues with the rest of the segment.

**Intent and rebuild.**
- `archive_prepare` returns the `PreparedSegment`: the intent, the five tables, the manifest, the quarantine files and the `YYYY/MM/DD` path. It records the intent through the existing `archive_intend`.
- Add `Store::archive_rebuild(&ArchiveIntent)`, which re-encodes exactly the intent's range. A different segment ID is a `STORAGE_FAILURE` that names both IDs.

**Operator example (`crates/sparrow-server/examples/archive_segments.rs`).**
- Usage: `archive_segments CONFIG OUT_DIR COUNT`.
- It opens a disposable store copy the way the `recovery` example does: no listeners, no watches, no maintenance. It prepares up to `COUNT` segments with `now` from the system clock, writes each segment's files under `OUT_DIR/<segment-id>/`, reads them back with `purrdf::columnar::read`, and checks every blob's SHA-256.
- It prints one JSON line per segment on stdout: `segment_id`, `first`, `last`, `records`, `artifacts`, `blobs`, `quarantined`, `verified_blobs` and `blake3`, an object keyed by table file name. After the last segment it prints one summary line on stderr, `{"segments":N,"peak_rss_kib":N}`, with the peak resident set size read from `VmHWM` in `/proc/self/status`. No `time` binary is installed on this host, so the example reports its own peak.
- It never commits a watermark.

**Docs.**
- `docs/archive-format.md`: mark P2 implemented. Replace the memory estimate in section 2 with the measured peak resident memory for a 64 MiB segment. Add the archive scope to section 4.
- `docs/interfaces.md`, "Archive-before-delete gate": describe `archive_prepare` and `archive_rebuild`.

## Out of scope

- Writing to the NAS, verifying there, committing the watermark, and archive status (archive-p3).
- Offline reader tools (archive-p4).
- Raising the segment caps before the PurRDF Parquet upgrade.

## Depends on

None. It uses the deletion gate, watermark and fence already on `main`.

## Automated acceptance

Tests in `crates/sparrow-core/tests/storage.rs`:

- `archive_segment_identity_is_deterministic`: the same range prepared twice, and on two copies of one store, gives the same segment ID and byte-identical tables and manifest.
- `archived_segment_round_trips_bytes_and_graphs`: `purrdf::columnar::read` returns every blob with a matching SHA-256 and every graph quad of the source records, artifacts and projections.
- `logically_expired_unretired_records_are_still_archived`: a record past its deadline and held by the gate is fully archived.
- `segment_over_the_codec_bound_is_refused_before_intent`: returns `RESOURCE_LIMIT`, and `archive_state` shows no intent.
- `unencodable_entries_are_quarantined_and_the_segment_continues`: one case each for `encode`, `projection` and `retired`, with manifest entries and file digests.
- `schedule_follows_age_size_backlog_and_horizon`: covers the 75% age boundary, the 75% size boundary, a pressure request, an open import preparation and an unsynchronized buffered entry holding the horizon.
- `segment_closes_at_the_utc_day_and_record_limits`.
- `rebuild_from_intent_matches_or_fails_loudly`: a changed range gives `STORAGE_FAILURE`.
- `manifest_contains_no_wall_clock_time`.

`make check` passes.

## UAT

Run on this host. The live service gains no new behavior in this phase, so the encoder runs on disposable copies of the newest closed-store backup. The live store holds entries received before 2026-10-10, past the 75% age point of the 30-day retention, so the backup has due entries.

0. Before deployment, record the store format; after deployment, set up two copies of the newest backup:
   ```bash
   SOCK=/SPARROW/store/rpc.sock
   SP="$HOME/.local/bin/sparrow $SOCK"
   mkdir -p -m 0700 ~/.local/state/sparrow-uat; $SP status | jq -r .store_format > ~/.local/state/sparrow-uat/p2-store-format
   B=$(ls -1d ~/.local/share/sparrow/backups/*/ | tail -1)
   U=~/.local/state/sparrow-uat/p2; mkdir -p -m 0700 $U
   for c in a b; do
     cp -a --reflink=auto $B/store $U/store-$c
     sed -e "s#^data_dir = .*#data_dir = \"$U/store-$c\"#" -e "s#^socket_path = .*#socket_path = \"$U/store-$c/rpc.sock\"#" ~/.config/sparrow/server.toml > $U/$c.toml
     printf '\n[archive]\ndir = "/net/synology/archives/sparrow-p2-uat"\n' >> $U/$c.toml
   done
   cd ~/Active/sparrow && cargo build --locked --release -p sparrow-server --example archive_segments
   X=~/Active/sparrow/target/release/examples/archive_segments
   ```
   Expected: both copies and configs exist.
1. Encode three segments from copy A:
   ```bash
   $X $U/a.toml $U/out-a 3 > $U/a.jsonl 2> $U/a.err; echo "exit=$?"
   jq -c '{segment_id, records, blobs, verified_blobs, quarantined}' $U/a.jsonl
   tail -1 $U/a.err | jq -c .
   ```
   Expected: `exit=0`; three lines; `verified_blobs` equals `blobs` on each; the summary shows `segments` `3` and `peak_rss_kib` below `1048576` (the target, 1 GiB).
2. Encode the same segments from copy B:
   ```bash
   $X $U/b.toml $U/out-b 3 > $U/b.jsonl
   diff <(jq -r .segment_id $U/a.jsonl) <(jq -r .segment_id $U/b.jsonl) && echo SAME-IDS
   ```
   Expected: `SAME-IDS`.
3. Check the table digests against the manifest:
   ```bash
   S=$(head -1 $U/a.jsonl | jq -r .segment_id)
   b3sum $U/out-a/$S/*.parquet
   grep -o 'blake3> "[0-9a-f]*"' $U/out-a/$S/manifest.nq
   ```
   Expected: each of the five digests that `b3sum` prints appears in the manifest.
4. Read the tables with DuckDB, without Sparrow:
   ```bash
   duckdb -c "select count(*) from read_parquet('$U/out-a/$S/blobs.parquet')"
   head -1 $U/a.jsonl | jq .blobs
   ```
   Expected: the two counts are equal.
5. Confirm the live service is unchanged:
   ```bash
   systemctl --user is-active sparrow.service
   $SP status | jq -r .store_format | diff - ~/.local/state/sparrow-uat/p2-store-format && echo FORMAT-SAME
   $SP status | jq -r .pressure.phase
   ```
   Expected: `active`; `FORMAT-SAME` (this phase adds no store-format family, and the live store is not fenced); `ready`.
6. Remove the copies:
   ```bash
   rm -rf ~/.local/state/sparrow-uat/p2 ~/.local/state/sparrow-uat/p2-store-format
   ```
   Expected: the copies are gone.

Done when every UAT step passes on the live service.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only

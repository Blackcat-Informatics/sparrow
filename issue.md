# Issue #17: Ship offline archive readers to list, verify and query segments

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Summary

Add `sparrow-archive`, an offline tool that lists, verifies and runs SPARQL over archived segments directly from the NAS, with no running Sparrow. The archive exists so aged-out logs stay usable after they leave the 30-day window. Without a reader, an operator cannot find evidence in it or prove it is intact, and corruption would go unnoticed until the data is needed.

## Current behavior

Verified on `main` at `ac6277f`.

- `docs/archive-format.md` section 10 describes two ways to read a segment.
  - PurRDF reads it with `purrdf::columnar::read`, which validates the tables and checks every blob's SHA-256.
  - DuckDB can query the tables with segment-local joins, for example `read_parquet('<root>/<store-hex>/segments/*/*/*/*/quads.parquet', filename = true)`.
  - It promises "an offline `sparrow-archive verify|list <dir>` tool" that checks `STORE.nq`, manifests, digests and quarantine files and reports corruption.
- Section 3 says a PurRDF revision change can produce duplicate segments for one range after a backup restore. Readers keep one copy per record IRI and report duplicates.
- No reader exists. `crates/sparrow-server/Cargo.toml:13-19` declares two binaries, `sparrowd` and `sparrow`. `make build-runtime` builds all of them (`Makefile:12-13`).
- PurRDF at the pinned revision `a47940e1` provides `purrdf::columnar::read` and governed SPARQL evaluation through the facade.

## Work

**Reader library (new `crates/sparrow-core/src/archive_read.rs`, no RocksDB use).**
- Walk `<root>/<store-hex>/` and parse `STORE.nq` and each `manifest.nq` with PurRDF's N-Quads parser into typed manifests: segment ID, positions, received range, counts, tables with BLAKE3, length and row count, and quarantine entries.
- Ignore `.staging/`.
- Share the vocabulary constants with the writer from one module, so the reader and writer cannot drift.

**Binary (`crates/sparrow-server/src/bin/sparrow-archive.rs`, a new `[[bin]]`).** Every command prints JSON lines on stdout, writes diagnostics to stderr, and exits nonzero on any failure, with no successful prefix.
- `sparrow-archive list ROOT [--store HEX] [--from DATE] [--to DATE]`: one line per segment with `store`, `segment_id`, `path`, `first`, `last`, `received_from`, `received_through`, `records`, `artifacts`, `blobs`, `quarantined` and `purrdf_revision`. `DATE` is `YYYY-MM-DD` in UTC and selects by directory date.
- `sparrow-archive verify ROOT [--store HEX] [--segment ID] [--from DATE] [--to DATE]` checks:
  - `STORE.nq` against the directory name;
  - each manifest's required facts;
  - each table's length and BLAKE3, by streamed reads;
  - each segment's identity, recomputed with `purrdf_hash::frame::frame_le` as section 3 defines;
  - `purrdf::columnar::read` validation, including every blob's SHA-256;
  - each quarantine file's BLAKE3, SHA-256 and length;
  - duplicates by record IRI across the selected segments.

  It prints one line per segment with `ok` and the failures found, then a summary line with `segments`, `ok`, `corrupt` and `duplicates`. Any corruption exits 1.
- `sparrow-archive query ROOT --sparql FILE [--store HEX] [--from DATE] [--to DATE] [--segment ID]`:
  - `FILE` is JSON with `query` and optional `limits` matching `SparqlLimits`.
  - Each selected segment is read with `purrdf::columnar::read` and verified first, then queried with PurRDF's governed SPARQL using those limits.
  - Rows from all segments are merged in segment order. Rows whose bound record IRIs come from a duplicate segment are kept once.
  - The output has `rows`, `segments_read`, `answer_certainty` and the governor stop reason when a limit is hit, in the live `sparql` command's shape: `rows.native_result` is PurRDF's SPARQL JSON result, so solutions are `rows.native_result.results.bindings`, and `answer_certainty` is `certain`, `at_most` or `unknown`.
  - Memory is bounded by one segment at a time plus the merged row limit.

**Docs.**
- `docs/archive-format.md` section 10: replace the promise with the commands. Add a worked DuckDB query, kept as `docs/archive-record-bytes.sql` and shown in the section, that finds a record's bytes by joining `quads`, `terms` and `blobs` within one segment. It reads the segment directory from `getvariable('segment')` and the record IRI from `getvariable('record')`, and returns one row, `record`, `sha256` and `byte_length`. The UAT below runs it.
- `docs/deployment.md`: in the NAS archive section, add a monthly `sparrow-archive verify` run and how to read its summary.

## Out of scope

- Re-importing archived evidence into a store.
- A long-running archive query service.
- Repairing corrupt segments.

## Depends on

The NAS archive worker issue (#16).

## Automated acceptance

Tests in `crates/sparrow-core` and `crates/sparrow-server`, over segments written by the archive-p2 encoder into a temporary root:

- `list_reports_every_segment_with_its_manifest_counts`.
- `verify_passes_an_intact_archive`.
- `verify_detects_table_corruption`: one flipped byte in each of the five tables in turn gives `corrupt` with the file named, and exit 1.
- `verify_detects_a_wrong_store_binding_and_a_truncated_manifest`.
- `verify_detects_quarantine_file_corruption`.
- `verify_reports_duplicate_segments_for_one_range`: two segments covering the same records (as after a PurRDF revision change) are reported as duplicates, not as corruption.
- `query_matches_the_live_store_for_the_same_records`: a metadata query over archived records gives the same rows as the store's `sparql` over those records before retirement.
- `query_is_governed`: a row-visit limit below the data size gives the governor stop reason and an uncertain answer, never a truncated certain one.

`make check` passes.

## UAT

Run on this host after the change is deployed, with archiving running on the live service (archive-p3).

0. Set up:
   ```bash
   SOCK=/SPARROW/store/rpc.sock
   SP="$HOME/.local/bin/sparrow $SOCK"
   STORE=$($SP status | jq -r .store_id); HEX=${STORE#store:}
   A=~/.local/libexec/sparrow/sparrow-archive; R=/net/synology/archives/sparrow
   U=~/.local/state/sparrow-uat/p4; mkdir -p -m 0700 $U
   ```
1. List the archive:
   ```bash
   $A list $R --store $HEX > $U/list.jsonl; echo "exit=$?"
   wc -l < $U/list.jsonl
   $SP status | jq -r .archive.session.segments
   ```
   Expected: `exit=0`; the line count is at least the number of segments the live status reports for this session.
2. Verify the whole archive:
   ```bash
   $A verify $R --store $HEX | tail -1
   ```
   Expected: `corrupt` is `0` and `ok` equals `segments`, with exit status 0.
3. Detect corruption in a copy:
   ```bash
   S=$(head -1 $U/list.jsonl | jq -r .path); REL=${S#$R/}
   mkdir -p $U/copy/$(dirname $REL) && cp $R/$HEX/STORE.nq $U/copy/$HEX/
   cp -r $S $U/copy/$(dirname $REL)/
   F=$U/copy/$REL/quads.parquet
   B=$(od -An -tu1 -j100 -N1 $F | tr -d ' ')
   printf "\\$(printf %03o $((255 - B)))" | dd of=$F bs=1 seek=100 conv=notrunc 2>/dev/null
   $A verify $U/copy --store $HEX; echo "exit=$?"
   ```
   Expected: the segment is reported `corrupt` with `quads.parquet` named, and `exit=1`.
4. Query archived records with SPARQL:
   ```bash
   cat > $U/q.json <<'EOF'
   {"query":"PREFIX sp: <urn:sparrow:vocab:> SELECT ?source (COUNT(?r) AS ?n) WHERE { ?r a sp:Record ; sp:sourceName ?source } GROUP BY ?source ORDER BY ?source"}
   EOF
   D=$(head -1 $U/list.jsonl | jq -r '.received_from[0:10]')
   $A query $R --store $HEX --from $D --to $D --sparql $U/q.json | jq '{rows: (.rows.native_result.results.bindings | length), answer_certainty, segments_read}'
   ```
   Expected: `rows` at least `1`; `answer_certainty` `certain`; `segments_read` at least `1`.
5. Cross-check with DuckDB, without Sparrow, using the documented join:
   ```bash
   SEG=$(head -1 $U/list.jsonl | jq -r .path); SID=$(head -1 $U/list.jsonl | jq -r .segment_id)
   duckdb -c "select count(*) from read_parquet('$SEG/blobs.parquet')"
   head -1 $U/list.jsonl | jq .blobs
   jq -n '{query: "PREFIX sp: <urn:sparrow:vocab:> SELECT ?r WHERE { ?r a sp:Record } ORDER BY ?r LIMIT 1"}' > $U/first.json
   RIRI=$($A query $R --store $HEX --segment $SID --sparql $U/first.json | jq -r '.rows.native_result.results.bindings[0].r.value')
   duckdb -csv -cmd "SET VARIABLE segment = '$SEG'; SET VARIABLE record = '$RIRI';" -f ~/Active/sparrow/docs/archive-record-bytes.sql
   ```
   Expected: the two counts are equal, and the worked query prints a header and exactly one row whose `record` is `$RIRI`.
6. Remove the scratch copy:
   ```bash
   rm -rf ~/.local/state/sparrow-uat/p4
   ```

Done when every UAT step passes on the live service.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only

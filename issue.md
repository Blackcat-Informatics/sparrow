TITLE: Make the artifacts listing answer on a store of live size instead of refusing every query

## Summary

On the live store (about 106,000 records, 13 GB), `sparrow artifacts STORE_ID` refuses every query with `RESOURCE_LIMIT: catalog capture exceeds operation admission`, whatever the source filter, the received-time window or the page size. A diagnostic surface that refuses every query cannot be used to find evidence. Make the listing read only the rows its page selects, so it answers at any store size, and keep an honest refusal only for a page that itself cannot fit its budget.

## Current behavior

Observed on 2026-11-02 against the running daemon, image `sparrowd-schema65-0221e87`, with the CLI:

- `sparrow SOCK artifacts STORE --source $SRC --received-from 2026-11-03T01:44:00Z --received-before 2026-11-03T01:46:00Z --page-size 5` prints `AgentError { code: "RESOURCE_LIMIT", message: "catalog capture exceeds operation admission", retryable: true }`.
- The same error comes back for another source, for a window that holds no artifacts, and with no source at all.
- `sparrow SOCK records --source $SRC --received-from ... --received-before ...` over the same window answers in about a second and returns each record's `artifact_id`, so records are the working path.

What the code does: the RPC handler `artifacts_list` (`crates/sparrow-server/src/rpc.rs:2955`) accepts the selector and window. The error text comes from `Writer::charge_catalog` and `charge_catalog_shape` (`crates/sparrow-core/src/storage.rs:953-990`), which charge every catalog row read against one fixed admission. The plain listing `Store::artifacts` (`crates/sparrow-core/src/storage.rs:2008`) scans every `artifact:` row of the store and collects them, with the filters applied after the scan. If the paged path behaves the same way, the charge grows with the store, not with the page.

## Work

1. Find where the paged listing charges rows outside its page. Start from the call chain of `artifacts_list` and from every `charge_catalog` caller it reaches.
2. Bound the read by the page: seek by the selected source and received-time range, stop at the page size, and charge only the rows read. The cursor continues from the last row.
3. Keep a typed `RESOURCE_LIMIT` for a page that cannot fit even one row, and make its message name the page size and the bound.
4. Answer whether the refusal predates schema 65. The code that raises this error predates the compression and large-artifact changes, so a store of this size may already have refused before them. Do not test it by starting a daemon on a restored copy, because that copy's watch catalog names the live intake roots. Establish it from the schema 62 behavior on the retained pre-upgrade backup with the recovery probe, or from the code history, and record the answer in this issue.

## Out of scope

- The `records`, `keys` and `values` queries, which work.
- Changing the store format.

## Depends on

Nothing.

## Automated acceptance

- `artifacts_listing_reads_only_its_page`: in `crates/sparrow-core/tests/storage.rs`, a store with more artifact rows than the old admission allows answers a one-page listing for a narrow window, and a second page continues from the cursor.
- `artifacts_listing_page_over_its_bound_is_refused_by_name`: a page that cannot fit one row returns `RESOURCE_LIMIT` naming the page size and the bound.
- `make check` passes.

## UAT

Run on this host after deployment.

0. Set up:
   ```bash
   SOCK=/SPARROW/store/rpc.sock
   SP="$HOME/.local/bin/sparrow $SOCK"
   STORE=$($SP status | jq -r .store_id)
   SRC=$($SP status | jq -r '[.consume_watches[] | select(.id | endswith("refusals"))][0].definition.source')
   ```
1. Narrow window with a source:
   ```bash
   $SP artifacts "$STORE" --source "$SRC" --received-from "$(date -u -d '-2 hour' +%FT%TZ)" --received-before "$(date -u -d '-1 hour' +%FT%TZ)" --page-size 5 | jq -c '{n: (.rows|length), cursor: .page.next_cursor}'
   ```
   Expected: exit 0 and a row count of at most 5, instead of `RESOURCE_LIMIT`.
2. No source, recent window:
   ```bash
   $SP artifacts "$STORE" --received-from "$(date -u -d '-30 min' +%FT%TZ)" --page-size 5 | jq -c '.rows | length'
   ```
   Expected: exit 0 and a count of at most 5.
3. Continue from a cursor:
   ```bash
   C=$($SP artifacts "$STORE" --source "$SRC" --received-from "$(date -u -d '-1 day' +%FT%TZ)" --page-size 5 | jq -r '.page.next_cursor // empty')
   [ -n "$C" ] && $SP artifacts "$STORE" --source "$SRC" --received-from "$(date -u -d '-1 day' +%FT%TZ)" --page-size 5 --cursor "$C" | jq -c '.rows | length'
   ```
   Expected: a second page of at most 5 rows.

Done when every UAT step passes on the live service.

Refs #146

This PR does not close #146. Done-when 3 (the measurement on a fresh copy of the live schema 68 store, with live intake replayed and query and status traffic running) is owed under #145, and it runs as the first work after this lands.

## What changed

- **Status and ingest stay responsive during a migration.**
  - Store-wide counters live in `store-counter:<name>` rows, kept in the same batch as every write. Status reads those rows instead of decoding every record under the writer lock.
  - Existing stores seed the counters through an online migration. Until it completes, status reports `counts: pending` and migration totals report `counting`; it never prints a partial number as a total.
- **Writers go ahead of migration pages.** A page waits for queued writers before it runs, including the first page after a retry and any page whose earlier wait went stale.
- **Pending counts reach every consumer.** The shutdown line, `sparrow-backup`, `user-unit-upgrade.sh` and `verify-backup.sh` all handle pending counts. `verify-backup.sh` exits 3 ("counts pending, not verified") instead of comparing a partial count. `verify-backup.sh` applies the live unit's Nice.
- **A poisoned writer refuses.** Status and the observed-status reads return STORAGE_FAILURE "catalog writer poisoned" without waiting for the lock.
- **An administrator can retire an unrecoverable claim** (coordination D118). The commands are `sparrow watch retire-claim WATCH CLAIM REASON` and the MCP tool `watch_claim_retire`.
  - It is refused on anonymous TCP, on the ingest socket and in query-only mode. It refuses a claim whose private source exists, one that published, one never claimed, one on another watch, and a second retirement. A store keeps at most 64 retired claims.
  - One batch records the time, caller and reason, drops the claim from pending, and ends its staged import's lease, so the reclaim pass frees the import slot and staged chunks.
  - Status, `watch list` and `ingest_get` show the claim as `retired-unrecoverable`. The daemon logs one line per retirement and clears the watch failure only when the retired claim caused it.
- The schema moves to 70 (wire: `countsPending`, `totalPending`, `watchClaimRetire`, `retiredClaims`).

## Review

Independent delivery review in five rounds (rounds 1 to 4 BLOCKED, round 5 `VERDICT: PASS`); the reports are in the Stage directory, `reviews/review-1.md` to `review-5.md`.

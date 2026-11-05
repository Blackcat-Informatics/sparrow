## comment 6066 paudley 2026-11-03T13:06:58Z 
 Answer to the first question, from the code history (no daemon was started): the refusal predates schema 65. The whole-store scan in the live listing arrived in 0f725e8 (schema 23), and the 64x per-row catalog charge in d9baf89 (schema 26). Since schema 62, `artifact_read.rs` and `catalog_lease.rs` changed only in their licence headers, and no catalog row format changed. A schema-62 daemon on a store of this size would refuse the same way.

The fix is on branch artifacts-list (5070e18), and review is next.
 

## comment 6092 paudley 2026-11-03T14:42:20Z 
 Paused 2026-11-03 at Patrick's development pause. Branch artifacts-list at 24a0484 (pushed): review round 2 PASS. Before merge: an O7 test (a source listing reads only its source) and O8 doc wording. Takes schema 66 and merges before #25. Nothing is merged or deployed. 


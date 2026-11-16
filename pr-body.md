Closes #340 (part one: count and journal). Part two, the 64 MiB staging limit itself, is a follow-up.

## Change

- **Counter.** `Status.stagingRefusals @62` (additive; the last field on main is @61) counts every refused RDF graph at the single point the staging limit is raised (`RdfWrite`, the 64 MiB write and 32 MiB dictionary limits). It reports the last refusal's limit, source, graph and anticipated bytes. One refused intake can add several, because a refused projection graph is absorbed and the batch can still fail later; the docs say so.
- **Journal.** One `sparrowd:` line per refusal, naming the limit, source, graph, anticipated bytes and running total. No record bytes. The observer writes with a write that ignores a failed write, so an unwritable stderr cannot panic while the store's writer lock is held (a panic there would poison the lock and refuse every later intake).

## Tests

- `an_intake_refused_for_rdf_staging_is_counted_and_named_in_status` (tests/storage.rs): an exact count of 4 for its input (three absorbed projection refusals, then the record's metadata graph), the observer totals 1 to 4, and the last refusal's limit, graph and bytes over 64 MiB. The count of 4 is observed from a probe run, not derived.
- `a_refusal_is_one_line_and_an_unwritable_journal_cannot_panic` (staging_refusal.rs): the write helper with a writer that always fails.

## Not tested

- A real stderr failure while the store's writer lock is held (needs a redirected stderr).
- The RPC integration encode path and `agent/status.rs` rendering, beyond the hook's full run.

## Evidence

Worktree `.stage/ingest-refuses-a-json-artifact-with-rdf/`: `commit.log` (5d40151), `commit2.log` (ee50bdb, EXIT=0). Review `.stage/archive/review-340.md`: BLOCKED at 5d40151 on the eprintln panic under the lock, the graph-versus-intake counting wording, and the thinning departing from the order; all three fixed at ee50bdb.

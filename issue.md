# Issue #13: Bound SPARQL work by the request's scope instead of the whole store

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Summary

Make a SPARQL, SPARQL-explain or correlation request read only the RDF rows of the graphs in its scope. Today every request scans index rows across the whole store and discards rows outside its scope only after charging them. On the live store, every SPARQL request tried on 2026-11-01 exhausted the 100,000-request ceiling, including a five-row query over one record. SPARQL is therefore unusable on the live service, and the v2 disk format's query parity cannot be checked there.

## Current behavior

Verified on `main` at `ac6277f` and against the live store on 2026-11-01 (89,631 live records).

**Observed.** Each of these requests failed with `RESOURCE_LIMIT: RDF read request, byte or row budget exhausted: attempted requests=100001/100000`:
- the metadata layer only, a `selection` scope of one record, and `SELECT * WHERE { ?s ?p ?o } LIMIT 5` (6.5 s);
- the metadata layer only, a three-record selection, and `PREFIX sp: <urn:sparrow:vocab:> SELECT ?r ?n WHERE { ?r sp:byteLength ?n }` (5.5 s);
- the metadata layer only, a ten-minute `received_from` window (about 110 records), and `?r a sp:Record ; sp:byteLength ?n` with `LIMIT 5` (12.4 s);
- all layers, a one-record selection, and `SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g` (7.9 s).

A request asking for `"limits":{"requests":"1000000","row_visits":"1000000"}` is refused with `query limits exceed server ceilings`: 100,000 requests and row visits are the host ceilings (`crates/sparrow-core/src/storage/rdf_view.rs:81-90`).

**Code path.**
- Index keys are `rdf:q:<stream>:<ORDER>:<four term IDs><owner>`, with the owner last (`crates/sparrow-core/src/storage/rdf_index.rs:265-272`).
- A pattern with any bound term reads `physical_rows` (`rdf_view.rs:405-440`), which seeks the prefix of bound terms in the chosen order. `QuadIter::next` (`:473-541`) charges one request per row (`:477`), then drops rows whose owner is outside the view's scope (`:511`). `?r sp:byteLength ?n` therefore reads every `byteLength` row in the store, about one per record and artifact.
- A pattern with no bound term uses `ScopedRows` (`rdf_view/sparse.rs:47-90`), which walks `graph_rows` (`rdf_view.rs:445-457`), every `rdf:g:<graph><owner>` declaration in the store, and drops those outside the scope only after charging them (`:543-597`, the owner check at `:592`).
- `probe_plan` (`:706-731`) chooses among `SPOG`, `POSG` and `OSPG` for the default graph, so the graph-leading orders `GSPO`, `GPOS` and `GOSP` are never used for a default-graph pattern. Every quad is written in all six orders (`rdf_index.rs:493-497`).
- The view's owner set is concrete and already bounded by scope admission (`view.owners`, filled at `rdf_view.rs:278`).

## Work

**Owner-to-graph index (`crates/sparrow-core/src/storage/rdf_index.rs`).**
- Write `rdf:o:<owner><graph>` beside every `rdf:g:<graph><owner>` declaration (`rdf_index.rs:259`), and delete it in the same batch wherever that declaration is deleted, through the same owner.
- At open, build the index for existing stores with a resumable job beside `rdf_index::upgrade_reverse` (`:232`), and fence the store with a new writer-format family in `writer_format.rs`.

**Scope-bounded evaluation (`crates/sparrow-core/src/storage/rdf_view.rs`, `rdf_view/sparse.rs`).**
- Whenever the request's scope restricts membership (a `selection` or `pin` scope, or a `live` scope with any source, time or diagnostic filter), evaluate every pattern per in-scope graph. Enumerate the graphs from `rdf:o:` for each owner in `view.owners`, then for each graph seek the graph-leading order whose next axes match the bound terms: `GSPO` when the subject is bound, `GPOS` when the predicate is bound, `GOSP` when only the object is bound, and `GSPO` when none is. Merge the per-graph cursors with the existing `ScopedRows` heap so rows keep one global order, and keep its union-default deduplication.
- `graph_rows` uses the same `rdf:o:` walk, so unbound patterns stop reading every graph declaration in the store.
- A request whose scope is the whole live store keeps today's plan; its cost is the store's by definition.
- Charge requests and row visits exactly as today. Only rows of in-scope graphs are read, so a request's cost follows its scope.
- The graph-leading orders stay in the physical index set. The v2 disk format issue (#5) runs its order census with this evaluation in place.

**Docs.** `docs/architecture.md` section 7: describe the owner-to-graph index and per-graph evaluation. `docs/interfaces.md`, the SPARQL section: state that request cost follows the scope's graphs and rows, not the store's size.

This changes the store format and no wire schema.

## Out of scope

- Raising the host ceilings.
- Query-time JSON projection (#5).
- Correlation-specific planning beyond sharing this evaluation.

## Depends on

None.

## Automated acceptance

Tests in `crates/sparrow-core/tests/storage.rs`:

- `scoped_sparql_cost_is_independent_of_store_size`: a store with 20,000 records answers a one-record selection query for `?s ?p ?o`, `?r sp:byteLength ?n` and a `GRAPH ?g` count with `requests` below 1,000, where today's plan exceeds 100,000. This protects scope-bounded cost.
- `scoped_sparql_answers_match_the_store_wide_plan`: across the existing SPARQL fixtures, scoped and unscoped plans return identical rows and certainty for metadata, diagnostic and content layers, including union-default deduplication and named-graph patterns.
- `owner_graph_index_tracks_graph_declarations`: after ingest, retirement and pin expiry, the `rdf:o:` rows equal the `rdf:g:` rows exactly.
- `owner_graph_index_migration_builds_the_index_and_fences_older_writers`.

`make check` passes.

## UAT

Run on this host after the change is deployed.

0. Set up:
   ```bash
   SOCK=/SPARROW/store/rpc.sock
   SP="$HOME/.local/bin/sparrow $SOCK"
   U=~/.local/state/sparrow-uat/sparql-scope; mkdir -p -m 0700 $U; cd $U
   STORE=$($SP status | jq -r .store_id)
   SRC=$($SP watch list | jq -r '[.watches[].definition | select(.format=="json")][0].source')
   FROM=$(date -u -d '-1 hour' +%FT%H:%M:00Z); BEFORE=$(date -u -d "$FROM +5 min" +%FT%TZ)
   $SP records --source $SRC --received-from $FROM --received-before $BEFORE --page-size 1000 > window.json
   N=$(jq -r .page.total_items window.json); RID=$(jq -r '.rows[0].record_id' window.json); echo "N=$N"
   ```
   Expected: `N` is at least `1`.
1. A one-record metadata query answers:
   ```bash
   jq -n --arg s "$STORE" --arg r "$RID" '{store_id: $s, scope: {kind: "selection", store_id: $s, record_ids: [$r]}, layers: {metadata: true, diagnostic: false, content: false}, query: "SELECT * WHERE { ?s ?p ?o } LIMIT 5"}' > one.json
   TIMEFORMAT=%R; time $SP sparql one.json | jq -c '{n: (.rows.native_result.results.bindings | length), answer_certainty}'
   ```
   Expected: `{"n":5,"answer_certainty":"certain"}` in under 5 seconds (the target).
2. A bound-predicate query over the window returns one row per record:
   ```bash
   jq -n --arg s "$STORE" --arg src "$SRC" --arg f "$FROM" --arg b "$BEFORE" '{store_id: $s, source: $src, received_from: $f, received_before: $b, layers: {metadata: true, diagnostic: false, content: false}, query: "PREFIX sp: <urn:sparrow:vocab:> SELECT ?r ?n WHERE { ?r a sp:Record ; sp:byteLength ?n }"}' > window-sparql.json
   $SP sparql window-sparql.json | jq -c '{n: (.rows.native_result.results.bindings | length), answer_certainty}'; echo "N=$N"
   ```
   Expected: `n` equals `N`, and `answer_certainty` is `certain`.
3. A content-layer graph count over one record answers:
   ```bash
   jq '.layers = {metadata: true, diagnostic: true, content: true} | .query = "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY ?g"' one.json > graphs.json
   $SP sparql graphs.json | jq -c '{n: (.rows.native_result.results.bindings | length), answer_certainty}'
   ```
   Expected: at least 2 graphs (the record's metadata and content graphs) and `certain`.
4. Confirm health:
   ```bash
   systemctl --user show sparrow.service -p NRestarts; $SP status | jq -r .pressure.phase
   ```
   Expected: `NRestarts=0` and `ready`.

Done when every UAT step passes on the live service.


## Comments (1)

### paudley — 2026-11-07T01:12:26Z

UAT addition from #10, which closed with this check moved here (coordination ruling 2026-11-06, 19:45 CST). After this change is deployed, the ingest-socket provenance predicates must answer over a one-record selection:

    SP="$HOME/.local/bin/sparrow /SPARROW/store/rpc.sock"; STORE=$($SP status | jq -r .store_id)
    jq -n --arg s "$STORE" '{store_id: $s, scope: {kind: "selection", store_id: $s, record_ids: ["record:42f45f85c40d006cd0bca49df06281aa"]}, layers: {metadata: true, diagnostic: false, content: false}, query: "PREFIX sp: <urn:sparrow:vocab:> SELECT ?uid ?gid WHERE { ?r sp:peerUid ?uid ; sp:peerGid ?gid }"}' > /tmp/sparrow-uat-peer.json
    $SP sparql /tmp/sparrow-uat-peer.json | jq -c '[.rows.native_result.results.bindings[] | {uid: .uid.value, gid: .gid.value}]'

Expected: `[{"uid":"951","gid":"948"}]`. On 2026-11-07 under schema 68 (image sparrowd-schema68-2aa56a9) this query, and a plain `SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 40` over the same selection, failed with RESOURCE_LIMIT `attempted requests=100001/100000`.

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only

# PurRDF integration gaps

These are unfinished first-version requirements, not exclusions from Sparrow's
scope. Sparrow must retain native semantics and bounded reads while resolving them.

## Retained operational SPARQL integration

The runtime selection and helper census reference now name committed PurRDF
`7734b013d19b68600ad24c4e8aa5a40955a8e89e`. That revision includes
`a77b743bcc6fad89d88581ef9cb5a626a326834f` (PR #526), which delivered the
bounded operational SPARQL ownership requested in
[issue #508](https://github.com/Blackcat-Informatics/purrdf/issues/508).
This source selection does not update the installed daemon.

Sparrow's persisted view implements the public `reserve_owned_workspace`
capability through its original shared read account. The erased reservation's
control box is admitted before PurRDF's public fallible allocation primitive
constructs it. Its fixed charge survives `resize(0)`. It retains the original
scope reservation, keeping the provider account admitted when a native complete,
partial or explanation result outlives the RocksDB snapshot callback. Payload
resizes use the same checked capacity owner as ordinary term/read guards;
releasing capacity never clears a sticky refusal. No snapshot or evidence pin is
implicitly renewed by retaining a result.

Operational results now carry PurRDF's retained ownership. Callers borrow the
native solution, boolean and graph accessors, or use ownership-carrying
extraction; they cannot move a raw result out and discard its admission. The
storage regressions compare the actual persisted four-form query outputs with a
resident oracle made by PurRDF's own `dataset_from_view`, confined to the test.
They also exercise representative algebra, retained solution extraction and
measured operational EXPLAIN. These checks and the required full gate must pass
before this integration is considered qualified.

Focused local qualification on 2026-10-17 passes four stored-view regression
tests, all sixteen core unit tests, and the long-text-query regression. The
seeded census self-tests include raw module identifier path resolution; the
pinned cross gate reports 260 ownership jobs, zero distinct exceptions and no
unsanctioned duplicate group. Logs are retained locally under
`.stage/native-graph-integration/`. These finite checks do not establish the
unfinished public graph surface or production-volume acceptance.

The former runtime revision `e58974e504b9681597faea1490002ce8ddaf7448`
admitted only projected SELECT/BGP without an explicit dataset. Its refusal of
GRAPH, ASK, CONSTRUCT, DESCRIBE, broader algebra, substitutions and extension
contexts was the upstream blocker; that historical refusal must not remain the
expected success condition of Sparrow's tests.

The complete production graph surface still requires scope capture and coverage,
prepared bindings, selected metadata/diagnostic/content graph layers, native text
relations and correlation, typed Cap'n Proto result publication, retained page
handles, cancellation and CLI/MCP parity. Complete, certain, at-most, unknown and
operational failure outcomes must retain the native distinction. A missing or
failed source checkpoint must never publish a successful result prefix. Measured
EXPLAIN must execute through the same bounded persisted view.

Do not suppress the bounded-view declaration, substitute a no-op reservation,
replay a selected corpus into a resident dataset in production, or copy the
private evaluator or workspace-pricing implementation. The complete SPARQL
CLI/MCP surface must not be advertised until its native admission and egress are
qualified.

Full qualification must compare resident and persisted answers for all four
forms, named/default graph selection, joins/modifiers/paths, RDF 1.2 layers and
terms, empty graphs, selected scope, retention/pins and failures during result
staging. It must separately verify certified governor stops and storage failures.
The dependency migration also propagates native fallible RDF builder operations
through Sparrow's typed error boundary (resource refusals remain RESOURCE_LIMIT)
and converts exact parsed timestamp
integers through a checked integer range. Ranking consumes PurRDF's bounded
score carrier at its full native value width. The removed native 1,024-query-term
ceiling is not reimplemented locally: query input remains limited to 64 KiB and
all existing posting/capture admissions apply. `limits.query_terms` is null
because there is no independent distinct-term ceiling; `query_text_bytes`
continues to report the effective input ceiling. A regression covers more than
1,024 distinct analyzed terms and independently refuses oversized input.

Forced physical reclamation has its separate unresolved binding requirements in
[native reclamation integration](native-reclamation.md).

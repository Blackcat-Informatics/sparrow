# PurRDF integration gaps

These are unfinished first-version requirements, not exclusions from Sparrow's
scope. Sparrow must retain native semantics and bounded reads while resolving them.

## Certified SPARQL workspace admission

The runtime and helper census pin PurRDF
`e58974e504b9681597faea1490002ce8ddaf7448`. Its public
`NativeSparqlEngine::query_governed_fallible_view` correctly composes storage
failures with native governor evidence. Its execution workspace admission in
`crates/sparql-eval/src/engine/bounded_workspace.rs::reserve_checked` currently
prices only SELECT with an unmodified projected basic graph pattern, without an
explicit active dataset. `check_inputs` also refuses configured extension contexts
and substitutions on bounded views.

Sparrow's real persisted view advertises `storage_live_budget()` and supplies
admitted term guards and operator reservations. A cross-record SELECT join works
over its virtual default union, without rebuilding a resident dataset. `GRAPH`,
ASK, CONSTRUCT and DESCRIBE refuse with
`native-sparql-workspace-unpriced`; modifiers, paths, expressions, aggregates,
explicit datasets and extension contexts also require native workspace pricing.
The live sibling's corresponding admission implementation was read again on
2026-10-08 from committed source at `341ad5ae2cecc5de026047f86dcd1194aff25f10`; it retains the same
limitation. Runtime and census pins are unchanged. No sibling source was changed.
Read-only verification at `aab23cbf20b68483ae58bf8440cd882bf1e1897b`
again finds this restriction; the admission file matches its committed source.

The upstream task is to extend certified workspace admission through the existing
fallible/governed execution path for these required forms and operators. It must
reserve before allocations, retain admission through result materialization,
compose read failures with governor evidence, preserve operational-failure
precedence, and certify native partial answers. A public host integration seam is
another possible upstream solution if it preserves those same guarantees.

Do not suppress the bounded-view declaration, substitute a no-op reservation,
replay a selected corpus into a resident dataset, or copy the private evaluator
or workspace-pricing implementation. Those approaches would leave the requested
stored-view and bounded-query behavior unmet. The complete SPARQL CLI/MCP surface
must not be advertised until its native admission and egress are qualified.

Qualification must compare resident and persisted answers for all four forms,
named/default graph selection, joins/modifiers/paths, RDF 1.2 layers and terms,
empty graphs, selected scope, retention/pins and failures during result staging.
It must separately verify certified governor stops and storage failures.

The public explanation APIs do not bypass this gap. At the pinned revision,
`explain_query_fallible_view` delegates to `explain_fallible_for`, which reserves
reporting, prepares the query and calls `reserve_fallible_workspace` before
`explain_prepared`. Explanation includes a metered measuring evaluation, not
just syntax or a plan description. Required unpriced forms therefore need the
same upstream admission extension before Sparrow can qualify `sparql_explain`.
Do not advertise a parser-only description as a native measured explanation.

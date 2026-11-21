# Issue #461: Render structured developer logs and capture selected console streams

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Compose with an application-owned tracing subscriber. Support log records and a Rust formatting macro named sparrow_eprintln! with captured identifiers. Present time, severity, target, message and fields in a scannable layout. Indent continuations, respect NO_COLOR and make redirected diagnostics readable. Preserve typed fields, span context and the stdout or stderr identity at intake. Offer explicit writers and tee selected child streams. Worker-driven output keeps slow terminal and transport I/O off the ordinary call path. Verify a single local display and single structured event, redirection and a slow sink.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only

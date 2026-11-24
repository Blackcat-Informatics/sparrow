# Issue #95: Enforce source bindings for every producer access class

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Bind exact source names or prefixes in `server.toml` to permitted Unix UIDs, ingest-socket peers, CHAP sessions or anonymous TCP. Validate the table when reading configuration. Apply a single admission decision to the private and ingest sockets, anonymous TCP and authenticated sessions. An unbound source keeps the access its endpoint currently allows.

The private socket obtains the kernel UID and PID for audit, and the ingest socket already carries typed peer credentials. Preserve typed identity through authorization. Apply bindings before ingest, event ingestion, sealed events and import staging. A denied write returns `PERMISSION_DENIED` naming the source and caller class and publishes nothing.

## Acceptance

- `bound_source_refuses_writes_from_other_access_classes` proves that a permitted Unix caller can write to a bound source and that anonymous TCP receives the typed refusal for that same source.
- `bound_source_refuses_another_uid` proves that another UID receives the typed refusal on the ingest socket before staging, while the permitted UID succeeds. Also verify the private socket path carries its kernel identity. #114 closes when its contract passes.
- Exercise an unbound source through an endpoint that already allows it. Document configuration and refusal in `docs/interfaces.md` and `docs/deployment.md`.

[Agreement on UID coverage](http://lillith:7555/fleet/sparrow/issues/114#issuecomment-9736)

## Shared Unix credential path

Carry typed kernel UID, GID and PID from the private and ingest sockets through the admission decision. Use the same credential for publication provenance on the private socket and keep its admin audit. #113 defines the artifact and record metadata acceptance. #114 owns the separate wrong-UID and permitted-UID checks on both Unix paths. Land these together where they share the credential path.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only

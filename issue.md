# Issue #57: Add the ingest keys to server.toml.example and intake to ArtifactReceipt

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

Row B-29 of docs/open-items.md. #10 review round 1, finding O4: pr/33:reviews/review-1.md:164-168, and pr/33:implementation.md:216-219. Marked by the implementer as "Not done".

## Current behavior

`deploy/config/server.toml.example` does not contain the three ingest keys (`ingest_socket_path`, `ingest_socket_gid`, `max_ingest_connections`; `crates/sparrow-server/src/config.rs:59-66`). The client `ArtifactReceipt` (`crates/sparrow-client/src/receipt.rs:129`) omits intake, while the typed `Artifact` and `Record` carry it.

## Work

Add the three keys, commented out with their defaults, to `deploy/config/server.toml.example`. Add an `intake` field to `ArtifactReceipt` and fill it from the receipt the daemon returns, or record in `docs/interfaces.md` why the receipt omits it.

## Done when

A test named `server_toml_example_documents_every_ingest_key` parses `deploy/config/server.toml.example` with the commented keys enabled and asserts the config loads. A client test named `artifact_receipt_carries_intake` publishes over the ingest socket and asserts the receipt's intake equals the caller credential. Both fail today.


## Comments (1)

### paudley — 2026-11-24T14:18:10Z

#55 covers the permissions required to bind the documented ingest socket
under the shipped system unit. This issue retains the three commented
configuration keys and a parser check with the example enabled.

The wire Artifact carried by the accepted reply already has optional
intake provenance. The SDK's ArtifactReceipt drops that field during
decoding. Expose the existing value and check an identified ingest
connection and an endpoint without intake metadata. #113 separately
tracks recording new provenance for the private socket.


## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only

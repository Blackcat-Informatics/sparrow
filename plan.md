# Plan #10: group-accessible ingest-only Unix socket with SO_PEERCRED provenance

Behavior: exactly the issue body (issue-10.md, "Work"): configuration keys and startup
directory/socket checks; one ingest gate shared by anonymous TCP and the ingest socket
(PERMISSION_DENIED for the socket, UNSUPPORTED for auth, TCP keeps UNAUTHENTICATED + CHAP);
peer credentials read on accept, carried on Access::IngestSocket into ingest calls, import bound
to the importBegin uid; optional intake provenance on Artifact and Record, four sp: predicates
in artifact and record metadata graphs, new writer-format family fence; wire OptionalIntake,
CLI context / MCP records_context render, status ingest-socket section; schema +1; tmpfiles
entry; docs/deployment.md and docs/interfaces.md updates.

Dependencies: none (issue). #7 producerWindowGet does not exist yet; nothing to add.

Exclusions (issue "Out of scope"): no credentials on the private socket, no source-to-uid
binding, no remote producers. Nothing wider than the issue (coordination ruling below).

Acceptance: the six automated tests named in the issue; make check (pre-commit hook). UAT on the
live service after deploy (step 0 needs sudo and a restart: Patrick's or coordination's action).

Risk: widens the 0600 trust boundary to one group, ingest only; store format fence; schema bump
requires a restart to deploy (separately authorized).

## User rulings

- 2026-11-06 15:25 CST (lillith_coordination, standing direction as Patrick; Patrick: "Have the
  agents assess and close/or finish all closeable items."): "Your order is set: #10 first
  (ingest-only, SO_PEERCRED provenance, exactly as the issue states, nothing wider), then #20,
  then #14; #11 and #13 after cutover; #22 and #25 stay on Patrick's window. Start #10 now.
  Closable: #31 on tomorrow's nightly as planned; #23 (test port collisions) if the fix is
  small, since it blocks commits for no reason. Report when #10 has a branch."

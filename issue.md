# Issue #55: Let the system unit and AppArmor profile bind the ingest socket

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

Row B-27 of docs/open-items.md. #10 review round 1, finding O2: pr/33:reviews/review-1.md:152-158. Marked by the reviewer; the implementer left it as "or a follow-up issue". #22 lists the ingest socket as out of scope.

## Current behavior

`deploy/systemd/sparrow.service:95-99` sets `ProtectSystem=strict` with `ReadWritePaths=/SPARROW/store /SPARROW/intake`, which does not allow the ingest socket path `/run/sparrow-ingest/`, and `deploy/apparmor/sparrow` has no rule for it. The system unit therefore cannot bind the ingest socket that `docs/deployment.md:173-180` tells the operator to configure. The live user unit has no such confinement.

## Work

Add the ingest socket directory to the system unit (a `RuntimeDirectory=` or `ReadWritePaths` entry matching `docs/deployment.md`) and an AppArmor rule for the socket path and `SO_PEERCRED` use. The example configuration keys are tracked separately under row B-29.

## Done when

The deploy rehearsal or staged-mode test named `system_unit_binds_the_ingest_socket` starts the system unit configuration with `ingest_socket_path` set and asserts the socket exists with the configured group and mode, and `apparmor_parser -Q` parses the updated profile. Today the unit's sandbox refuses the bind, so the test fails.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only

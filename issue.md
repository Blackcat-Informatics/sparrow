# Issue #463: Package the existing command line and MCP adapters in sparrow-access

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Move the existing sparrow executable and stdio MCP adapter to the sparrow-access package. Keep existing arguments, exit status and output contracts. Move the applicable build and install targets and command-level tests. Keep search on the server and query commands in access. Add a command to drain an inactive producer spool using the same delivery and receipt certification as #7. Verify command compatibility and that an external producer can use sparrow-client independently.


## Comments (1)

### paudley — 2026-11-21T23:17:38Z

The current binary handles endpoint parsing, CLI JSON output, the exit code for failed queries, and MCP stdio selection. Move that entry point with the command dispatcher and the MCP adapter. Several wire modules contain both daemon encoders and access-side renderers. Keep one owner for each shared conversion and for watch validation, and preserve the producer crate's independent dependency graph.

The mixed RPC integration suite invokes both executables using Cargo's package-local binary environment. Moving the binary requires updating the fixture so a clean, focused test still builds and locates both real entry points. One option is a non-shipping integration package with distinctly named fixture binaries pointing to the production entry points. Keep command output, failed-query exit and MCP framing under test. Update the shipped-crate check, runtime build and install selections. Preserve the installed executable name and path.


## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only

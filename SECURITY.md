<!-- SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca> -->
<!-- SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0 -->

# Sparrow Security Policy

Sparrow (the diagnostic evidence service, its client library, CLI and stdio MCP
adapter) is maintained by Blackcat Informatics® Inc. Please report
vulnerabilities privately so they can be investigated and fixed responsibly.

## Supported versions

Security fixes are available on `main`. Tagged releases are source
snapshots. To apply a fix, update to the `main` revision that contains it.

## Reporting a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/Blackcat-Informatics/sparrow/security/advisories/new)
or email <security@blackcatinformatics.ca> with `SECURITY` and `Sparrow` in the
subject line.

Include the following in your report.

- name the affected component (server, client library, CLI, MCP adapter or
  packaging) and the commit you tested
- describe the vulnerability, its impact and the configuration it needs
- provide reproduction steps, a proof-of-concept input or a patch when possible

## Threat surface

Sparrow runs as an unprivileged Linux service with local storage. The
surfaces most relevant to a report are:

- **The group-accessible Unix socket.** The store directory uses mode
  `0710` and the socket uses mode `0660`. The service group can traverse
  the directory and connect to the socket. The example service keeps store
  files owner-only through its umask. On a writable daemon, socket access
  includes administrative RPCs for pins and deletion-capable watches.
  Treat service group membership as administrative access. Report permission
  races, symlink substitution and socket access outside the owner and
  service group.
- **Public Cap'n Proto TCP.** When configured, the TCP listener admits
  anonymous peers for discovery, ingestion, flush and imports only. Every
  other operation requires a shared-secret CHAP session. **CHAP
  authenticates only the start of a session. It does not encrypt traffic or
  authenticate the server.** An on-path attacker can observe or take over an
  authenticated connection, and an observer of one exchange can test secret
  guesses offline. Use a random secret file, never a passphrase, and restrict
  the listener to trusted networks. Anonymous ingestion and imports consume
  the disk budget and share the import journal pool by design. Report bypasses
  of the anonymous admission list or of CHAP, not these documented limits.
- **Consume watches delete source files.** A `consume` watch removes each
  claimed file once its complete bytes and catalog entry are durable. Any
  client that can administer watches (Unix socket or an authenticated TCP
  session) can therefore direct the service to delete files that its user can
  write. Watches must not follow symlinks, must exclude Sparrow's own data,
  and must never delete a new file that replaced a claimed path. Deleting
  anything else, or deleting before durability, is a vulnerability.
- **Parsing untrusted input.** JSON/JSONL, Markdown, syslog (TCP and UDP) and
  free text arrive from producers Sparrow does not control. Resource
  exhaustion, unbounded allocation and crashes on hostile input are in scope.
  RDF, SPARQL and text-analysis semantics come from
  [PurRDF](https://github.com/Blackcat-Informatics/purrdf); report flaws in
  those to PurRDF unless Sparrow's integration causes them.
- **Queries and retention.** Query governors, disk-budget admission, pin
  deadlines and eviction protect the service from exhaustion and must not
  expose or delete data outside their documented rules.

## What to expect

- acknowledgment within 48 hours
- initial triage within 7 days
- coordinated remediation and disclosure after validation

## Responsible disclosure process

1. Report the vulnerability privately.
2. Maintainers validate and triage the report.
3. A fix is developed, reviewed and tested.
4. The fix lands on `main` with an advisory where warranted.
5. Public disclosure follows after operators have had reasonable time to
   update.

## Reporter credit

Advisories credit the reporter unless the reporter asks for anonymity. Credit
uses the name, handle or organization the reporter supplies.

There are currently no publicly disclosed Sparrow vulnerabilities.

## Contact

- Email: <security@blackcatinformatics.ca>
- PGP key for encrypted reports:
```text
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v2

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
=fCTf
-----END PGP PUBLIC KEY BLOCK-----
```

For non-security questions or bug reports, use the normal public issue tracker.

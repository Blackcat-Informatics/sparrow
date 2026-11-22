<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->
## Summary

<!-- What does this change, and why? Link the issue if there is one. -->

## Checklist

- [ ] `make check` passes (fmt, release Clippy with warnings denied, release tests, census self-tests and the pinned cross gate)
- [ ] Commits went through the `make install-hooks` pre-commit gate; no verification was bypassed
- [ ] Each new or adopted helper has a `helpers-ledger.ttl` job, and new helpers reuse existing PurRDF functions where available
- [ ] New tests protect a named behaviour, regression, boundary or invariant
- [ ] Docs updated where the change is user-visible (`docs/interfaces.md` for RPC, MCP or CLI changes)
- [ ] I license this contribution under MIT OR Apache-2.0 OR MulanPSL-2.0, as described in `LICENSING.md`

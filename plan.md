# Plan: PurRDF group A head move with the Markdown profile migration (#28, D69, D74)

## Behavior
Sparrow's runtime pins (workspace dependencies purrdf, purrdf-hash, purrdf-lex, purrdf-ed25519
and purrdf-core) and its census reference move to PurRDF c6d670c762842541075bb28c424265ce074d4699
(H). PurRDF fb02dd38b changes the Markdown contract id, and so the stored RDF profile identity.
Store::open advances a store whose rdf_profile equals the pinned pre-H value (1909d176…) to
the current one, in one synced write, when the store holds no Markdown projection state. A
store that holds any refuses UNSUPPORTED, naming the count, and writes nothing.

## Dependencies
H landed in memetics (dc5eeec6). The live store's one Markdown record retires after
2026-11-06T06:16:07Z.

## Exclusions
Re-projecting stored Markdown records (option A) is out, by D74. No patch tables.

## Acceptance
- A test pins the fresh store's rdf_profile and text_analyzer (review O1), and shows the
  advance, the refusal with the count and no write, and the advance after retirement.
- The repository gate (the commit hook) passes.
- Before the restart, a reflink copy of the latest nightly opens under the H image with
  verify-backup (query-only), after the Markdown record retires.
- The restart after 2026-11-06T06:16Z, with a named pre-upgrade backup and a go notice.

## Risk
Persisted identity on the live store. Older backups holding the Markdown record restore only
under their own image.

## User rulings
- 2026-11-04 (lillith_memetics, relaying D69): "Move after that, by your own hook and review. A failure is fixed forward in its owning repository, never by pinning back."
- 2026-11-04 (coordination, decided): "the combined restart carries #24, #25 and the no-copy-on-write conversion on main's current pin a47940e, and this H move becomes its own delivery, ending in a second short restart."
- 2026-11-05 (coordination, D74): "go with B, recorded as D74. Keep the refusal path exactly as you described (any remaining Markdown projection refuses, naming the count, changes nothing), add the O1 identity-pinning test, and run the H restart after 2026-11-06T06:16Z with a named pre-upgrade backup and a go notice as before. Note in #28 that older backups holding the record restore only under their own image. That restart retires R38. Patrick may overrule; I have told him."

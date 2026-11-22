<!-- SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca> -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Sparrow Brand

## Name

The project name is **Sparrow**: a small, quick bird that the PurRDF cat
carries. Write it as `Sparrow` in prose and titles. Never write `SPARROW`, or
`sparrow` at the start of a sentence.

Machine identifiers stay lowercase `sparrow`: the crates (`sparrow-core`,
`sparrow-server`, `sparrow-client` and the dev-only crates), the `sparrow`
client binary and the `sparrow.service` user unit.

## Positioning

Sparrow should read as a dependable, bounded evidence service for agents. It
retains diagnostic logs and data for a fixed window and makes them searchable
as a semantic graph. It is not a dashboard, a metrics system or a
general-purpose database. Its claims are measured and its limits are
explicit: coverage, refusals and partial results are reported, never hidden.

## Tagline

Temporal diagnostic evidence, carried gently: retained, pinned and
queryable as a graph.

## Repository description

Sparrow: a Rust service that retains diagnostic logs and data for a bounded
window, deduplicates and compresses them, keeps named pins, and serves
full-text and SPARQL queries over PurRDF to agents through Cap'n Proto RPC,
MCP and a CLI.

## Voice

- Plain, exact and calm. State what is implemented, what was measured, and
  what is still unverified.
- Prefer concrete limits ("retired at 30 days from acceptance once archived") to adjectives
  ("long retention").
- No hype words, no exclamation marks and no promised performance without a
  cited measurement.
- The cat is the steward and the sparrow is the evidence it carries. Keep
  the imagery gentle: the cat holds the sparrow; it does not hunt it.

## Typography

- Use the platform's default sans-serif text for prose. Do not ship a custom
  font.
- Set identifiers, commands, paths and configuration keys in code style.
- Title the README with the plain `Sparrow` heading under the centred logo;
  do not render the name inside the logo.

## Family system

Sparrow reuses the black-cat mark of the PurRDF and gmeow family (see
[PurRDF's brand guide](https://github.com/Blackcat-Informatics/purrdf/blob/main/docs/BRAND.md)).
Keep the family recognisable by copying the shared `cat-head-core` SVG group
verbatim and swapping only the **service object** the cat holds.

This repository's service object is `service-sparrow`: a small sparrow held
in the cat's mouth. The cat's `mouth-bite` group is drawn over the bird's
back, so the fangs read as a gentle carry. The bird has a paper body, an ink
crown and bib, two ink wing bars, a cocked fan tail, tucked feet, a red eye
stripe and a yellow beak. It replaces PurRDF's `service-triple`, because
Sparrow is the service that carries PurRDF's graph semantics over retained
evidence.

The accent colours are isolated to the `service-sparrow` group for theming.
Only red (eye stripe) and yellow (beak) are used; blue and green stay
reserved.

## Colour tokens

- cat / ink: `#111214`
- paper (bird body, wing, tail): `#fffdf5`
- feature (cat eyes, ears, whiskers): `#ffffff`
- accents: red `#ea4335`, blue `#4285f4`, yellow `#fbbc05`, green `#34a853`

These are the PurRDF tokens unchanged. Do not introduce a brown for the
sparrow; the red eye stripe stands in for a house sparrow's chestnut.

## Logo rules

- Do not redraw, recolour or reproportion `cat-head-core`. Edit only
  `service-sparrow`.
- Keep the soft white glow filter so the black silhouette stays legible on
  dark backgrounds.
- The bird is scaled and placed by one `transform` on `service-sparrow`.
  Adjust that transform rather than every coordinate.
- Keep the SVG hand-editable: named groups, class-based colour tokens, and no
  embedded raster images or fonts.
- Minimum display size is 64 px. Below that, the bird's features merge.
- Do not add the Blackcat Informatics name or other marks to the logo.

## Logo assets

- `docs/sparrow-logo.svg`: the canonical Sparrow logo (cat and sparrow).

Use the SVG for README, icon and card placements. The README references it
by relative path so branch previews show the branch's current logo.

Check the SVG and preview it after editing:

```bash
xmllint --noout docs/sparrow-logo.svg
rsvg-convert -w 512 -h 512 docs/sparrow-logo.svg -o /tmp/sparrow-logo.png
```

A social-preview card is not yet drawn. When one is added, keep its editable
SVG source beside the rendered 1280×640 PNG, as PurRDF does.

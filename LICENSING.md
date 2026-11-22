<!-- SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca> -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Licensing

Blackcat Informatics® Inc. holds the copyright in Sparrow's first-party
material (© 2026). Third-party dependencies keep their own ownership and
grants. Two distinct things are true about Sparrow's licensing:

1. All first-party code is offered under **three open-source licenses, at
   your option**: MIT, Apache-2.0, or MulanPSL-2.0. This is the same offer
   PurRDF makes.
2. Separately, Blackcat Informatics® reserves the right, as copyright
   holder, to grant commercial or proprietary licenses outside those terms.

## Open-source terms

First-party code in the Rust workspace, the Cap'n Proto schema, packaging,
configuration examples, tests and build tooling is offered under your choice
of any one of:

| License | Text |
|---|---|
| **MIT** | [`LICENSE-MIT`](./LICENSE-MIT) |
| **Apache License 2.0** | [`LICENSE-APACHE`](./LICENSE-APACHE) |
| **Mulan Permissive Software License, Version 2 (MulanPSL-2.0)** | [`LICENSE-MULAN`](./LICENSE-MULAN) |

This is the grant in first-party source headers and crate metadata:

```text
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
```

Some documentation files instead carry `SPDX-License-Identifier: CC-BY-4.0`.
Those are still first-party Blackcat Informatics® material, licensed under
[Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/).

The [`LICENSES/`](./LICENSES/) directory holds each license text under its
SPDX identifier, following the [REUSE](https://reuse.software/) layout. The
root `LICENSE-*` files are the same texts under their conventional names.

### About MulanPSL-2.0

MulanPSL-2.0 is an [OSI-approved](https://opensource.org/license/MulanPSL-2.0)
permissive license published in Chinese and English. An adopter should know
these terms:

- Its Chinese and English versions have equal legal effect; **the Chinese
  text governs if they conflict or differ** (§6). The committed
  [`LICENSE-MULAN`](./LICENSE-MULAN) carries both languages.
- Each contributor grants a **patent license** within the scope and
  conditions of §2. It terminates from the date you or your affiliates
  start patent-infringement litigation, or another patent enforcement
  action, alleging that the software or a contribution infringes a patent.
  The full §2 controls the grant and its conditions.
- It grants **no trademark license except use necessary for §4 notices**
  (§3).
- Unlike Apache-2.0, it does **not** require stating changes to modified
  files.

The committed text is byte-identical to PurRDF's (SHA-256
`eb7a1d713eb919b146787629e22e4c975cb701f529a65d4d7e0fcd417558bf1c`, from the
SPDX license-list corpus).

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in Sparrow shall be licensed as above
(MIT OR Apache-2.0 OR MulanPSL-2.0), without any additional terms or
conditions.

## Dependencies and referenced data

Sparrow vendors no third-party source or data. Cargo dependencies, including
PurRDF, keep their own licenses and are fetched at build time.

The test suite reads PurRDF's text-analysis lexicons from a sibling PurRDF
checkout. Sparrow does not copy or redistribute them; their provenance and
upstream licenses are recorded in PurRDF.

The repository gate uses [`cargo-raf-census`](https://crates.io/crates/cargo-raf-census)
as a separately installed development tool. Its license is
MIT OR Apache-2.0 OR MulanPSL-2.0.

## Proprietary / commercial licensing

The open licenses above are offered **in addition to, not in place of,**
Blackcat Informatics®' right, as copyright holder, to license the software
under separate commercial or proprietary terms. Granting the open licenses
does not revoke or limit this reservation.

To obtain a proprietary license, contact **licensing@blackcatinformatics.ca**.

## Trademarks

"Blackcat Informatics®" is a registered trademark of Blackcat Informatics®
Inc. None of the open licenses grants any right to use this name, its logos,
or marks; see **Apache License 2.0 §6** and **MulanPSL-2.0 §3**. Nominative
references (for example, "built on Sparrow") are permitted; uses implying
endorsement or origin are not.

## Contributions

Contributions to Sparrow are accepted under **MIT OR Apache-2.0 OR
MulanPSL-2.0**. For the commercial reservation above to extend to
contributed material, contributors agree to license their contributions to
Blackcat Informatics® Inc. under terms that permit relicensing, including
under proprietary terms. A Contributor License Agreement may be required
before substantial contributions are merged. See
[`CONTRIBUTING.md`](./CONTRIBUTING.md).

## Copyright notice

> Copyright © 2026 Blackcat Informatics® Inc. All rights reserved, except as
> expressly granted under the licenses above.

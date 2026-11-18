<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Sparrow glossary

Each entry gives the term's meaning in Sparrow and the document that governs it.
The governing document wins over this page. The charter is
the [Purpose section of AGENTS.md](../AGENTS.md#purpose).

| Term | Meaning | Governing document |
| --- | --- | --- |
| Archive | The large, slower store (a NAS) to which aged-out evidence is written as GTS segment files before it is deleted. It is implemented and is not running on the live daemon. | [Archive format](archive-format.md#archive-status) |
| Artifact | One accepted complete file, dump, document or upload, with an occurrence ID, receipt time, media type, provenance and an exact-byte payload or manifest. | [Architecture, section 3](architecture.md#3-logical-model-and-identity) |
| Cruft | What `lillith_memetics` sends Sparrow as material it has set aside. Sparrow stores it as an ordinary artifact or record, with no special path. | [Interfaces, Cruft](interfaces.md#cruft) |
| Deletion gate | The single check that every expiry and eviction passes: nothing is retired unless the archive watermark covers it. | [Archive format, section 7](archive-format.md#7-deletion-gate-and-store-fence) |
| Disk budget | The store's configured byte allowance, with the maintenance reserve and the free-space floor. Ingest above it is refused with `STORAGE_PRESSURE`. | [Architecture, section 4.3](architecture.md#43-budgets-and-reclamation) |
| Event time basis | Whether a record's event time is a source instant (`source`) or its receipt time derived when read (`receipt`). Expiry never uses event time. | [Architecture, section 3](architecture.md#3-logical-model-and-identity); [Interfaces, receipt-time event fallback](interfaces.md#receipt-time-event-fallback-schema-69) |
| Fate | Where one item of cruft is and how long it stays. While the item is live, Sparrow finds it by source, by an exact JSON field value, or, for a whole artifact, by its BLAKE3 payload id; once archived, `sparrow-archive` lists and extracts it. | [Interfaces, Cruft](interfaces.md#cruft) |
| Fence | The store format change at first open with an archive policy that makes older binaries refuse the store, so no writer can bypass the gate. | [Archive format, section 7](archive-format.md#7-deletion-gate-and-store-fence) |
| Ingest socket | The optional group-accessible Unix socket that admits producer operations only and records each caller's uid, gid and pid in provenance. | [Interfaces, section 5](interfaces.md#5-capn-proto-ingestion-queries-and-administration) |
| Payload | An exact byte sequence identified by its full BLAKE3 digest, stored once however many records share it. | [Architecture, section 3](architecture.md#3-logical-model-and-identity) |
| Payload id | `blake3:` and the 64 lower-case hex digits of the BLAKE3 hash of an artifact's bytes. `payloadLookup` returns the artifacts that hold it. | [Interfaces, payload id index](interfaces.md#payload-id-index-store-format) |
| Pin | A named, expiring collection of artifact and record IDs that delays their deletion. Reads do not extend it. | [Architecture, section 5](architecture.md#5-retention-and-named-pins); [Interfaces, section 4](interfaces.md#4-named-pin-example-and-rules) |
| Projection | Derived RDF or text facts for a record, identified by content, codec and interpretation context, produced by PurRDF. | [Architecture, section 3](architecture.md#3-logical-model-and-identity) |
| Query-only daemon | A daemon started with `--query-only`: it opens a store to serve reads only and starts no consume or tail worker, syslog listener, TCP listener or maintenance worker. | [Interfaces, query-only daemon](interfaces.md#query-only-daemon-schema-67) |
| Razor | The Katamari/Sparrow rule for where data lives: whatever needs long-term storage, privacy, encryption, validation or resilience goes to Katamari, and everything else goes to Sparrow. | [Architecture, section 1](architecture.md#1-goals-and-boundaries) |
| Receipt time | The server instant at which Sparrow accepted an artifact or record. Retention and expiry use it alone. | [Architecture, section 5](architecture.md#5-retention-and-named-pins) |
| Record | One addressable log event or JSONL entry, with its own occurrence ID, source byte range and payload references. | [Architecture, section 3](architecture.md#3-logical-model-and-identity) |
| Retention window | An unpinned item is due at `received_at + 30 days` and is retired then once the archive holds it. Disk pressure can retire archived, unpinned items earlier; a pin keeps an item until the pin ends; the deletion gate keeps every unarchived item past its deadline, which on the live daemon today is every item. There is one global policy. | [Architecture, section 5](architecture.md#5-retention-and-named-pins) |
| Segment | One immutable GTS file of the archive, named by the first and last eviction positions it holds. | [Archive format, section 1](archive-format.md#1-layout) |
| Source | A logical intake origin, such as a configured watch, syslog listener or client, that groups artifacts and stream generations. | [Architecture, section 3](architecture.md#3-logical-model-and-identity) |
| Store id | The persistent identity of one store, `store:<hex>`, that scopes every artifact, record, source, stream and pin ID. The archive holds one store per root. | [Architecture, section 3](architecture.md#3-logical-model-and-identity); [Archive format, section 1](archive-format.md#1-layout) |
| Stream | An ongoing source such as an active file or syslog stream. It describes provenance and ordering and does not retain unlimited history. | [Architecture, section 3](architecture.md#3-logical-model-and-identity) |
| Watermark | The durable `archive:through` position: the last eviction position written to the archive and verified. Nothing after it is deleted. | [Archive format, section 7](archive-format.md#7-deletion-gate-and-store-fence) |

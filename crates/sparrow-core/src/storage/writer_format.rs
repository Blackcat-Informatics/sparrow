// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Preserve the original authority family beneath the artifact-RDF writer fence.

use super::Error;
use rocksdb::{DB, WriteBatch};

// Each new identity adds artifact RDF to exactly one existing authority family.
const BASE: [&[u8]; 11] = [
    b"sparrow.store/33",
    b"sparrow.store/34",
    b"sparrow.store/35",
    b"sparrow.store/36",
    b"sparrow.store/37",
    b"sparrow.store/38",
    b"sparrow.store/39",
    b"sparrow.store/40",
    b"sparrow.store/41",
    b"sparrow.store/42",
    b"sparrow.store/43",
];
const RDF: [&[u8]; 11] = [
    b"sparrow.store/44",
    b"sparrow.store/45",
    b"sparrow.store/46",
    b"sparrow.store/47",
    b"sparrow.store/48",
    b"sparrow.store/49",
    b"sparrow.store/50",
    b"sparrow.store/51",
    b"sparrow.store/52",
    b"sparrow.store/53",
    b"sparrow.store/54",
];
const ALL_RDF: [&[u8]; 11] = [
    b"sparrow.store/55",
    b"sparrow.store/56",
    b"sparrow.store/57",
    b"sparrow.store/58",
    b"sparrow.store/59",
    b"sparrow.store/60",
    b"sparrow.store/61",
    b"sparrow.store/62",
    b"sparrow.store/63",
    b"sparrow.store/64",
    b"sparrow.store/65",
];
const PROVENANCE: [&[u8]; 11] = [
    b"sparrow.store/66",
    b"sparrow.store/67",
    b"sparrow.store/68",
    b"sparrow.store/69",
    b"sparrow.store/70",
    b"sparrow.store/71",
    b"sparrow.store/72",
    b"sparrow.store/73",
    b"sparrow.store/74",
    b"sparrow.store/75",
    b"sparrow.store/76",
];

const TRACING: [&[u8]; 11] = [
    b"sparrow.store/77",
    b"sparrow.store/78",
    b"sparrow.store/79",
    b"sparrow.store/80",
    b"sparrow.store/81",
    b"sparrow.store/82",
    b"sparrow.store/83",
    b"sparrow.store/84",
    b"sparrow.store/85",
    b"sparrow.store/86",
    b"sparrow.store/87",
];

const LOG_TIME: [&[u8]; 11] = [
    b"sparrow.store/88",
    b"sparrow.store/89",
    b"sparrow.store/90",
    b"sparrow.store/91",
    b"sparrow.store/92",
    b"sparrow.store/93",
    b"sparrow.store/94",
    b"sparrow.store/95",
    b"sparrow.store/96",
    b"sparrow.store/97",
    b"sparrow.store/98",
];

// Archive-before-delete: older binaries cannot retire evidence the archive has not covered.
const ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/99",
    b"sparrow.store/100",
    b"sparrow.store/101",
    b"sparrow.store/102",
    b"sparrow.store/103",
    b"sparrow.store/104",
    b"sparrow.store/105",
    b"sparrow.store/106",
    b"sparrow.store/107",
    b"sparrow.store/108",
    b"sparrow.store/109",
];

// Ingest-socket intake provenance: older binaries cannot drop a record's caller identity.
// Like the archive family it includes the log time law. Archiving is a separate switch, so
// an intake store that also archives has its own family.
const INTAKE: [&[u8]; 11] = [
    b"sparrow.store/110",
    b"sparrow.store/111",
    b"sparrow.store/112",
    b"sparrow.store/113",
    b"sparrow.store/114",
    b"sparrow.store/115",
    b"sparrow.store/116",
    b"sparrow.store/117",
    b"sparrow.store/118",
    b"sparrow.store/119",
    b"sparrow.store/120",
];
const INTAKE_ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/121",
    b"sparrow.store/122",
    b"sparrow.store/123",
    b"sparrow.store/124",
    b"sparrow.store/125",
    b"sparrow.store/126",
    b"sparrow.store/127",
    b"sparrow.store/128",
    b"sparrow.store/129",
    b"sparrow.store/130",
    b"sparrow.store/131",
];

// Receipt-time event fallback RDF: older binaries cannot publish a record metadata graph
// without its `eventTimeFallback`. Each family lifts one family that existed before it,
// keeping that family's log time, archive and intake switches.
const FALLBACK_TRACING: [&[u8]; 11] = [
    b"sparrow.store/132",
    b"sparrow.store/133",
    b"sparrow.store/134",
    b"sparrow.store/135",
    b"sparrow.store/136",
    b"sparrow.store/137",
    b"sparrow.store/138",
    b"sparrow.store/139",
    b"sparrow.store/140",
    b"sparrow.store/141",
    b"sparrow.store/142",
];
const FALLBACK_LOG_TIME: [&[u8]; 11] = [
    b"sparrow.store/143",
    b"sparrow.store/144",
    b"sparrow.store/145",
    b"sparrow.store/146",
    b"sparrow.store/147",
    b"sparrow.store/148",
    b"sparrow.store/149",
    b"sparrow.store/150",
    b"sparrow.store/151",
    b"sparrow.store/152",
    b"sparrow.store/153",
];
const FALLBACK_ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/154",
    b"sparrow.store/155",
    b"sparrow.store/156",
    b"sparrow.store/157",
    b"sparrow.store/158",
    b"sparrow.store/159",
    b"sparrow.store/160",
    b"sparrow.store/161",
    b"sparrow.store/162",
    b"sparrow.store/163",
    b"sparrow.store/164",
];
const FALLBACK_INTAKE: [&[u8]; 11] = [
    b"sparrow.store/165",
    b"sparrow.store/166",
    b"sparrow.store/167",
    b"sparrow.store/168",
    b"sparrow.store/169",
    b"sparrow.store/170",
    b"sparrow.store/171",
    b"sparrow.store/172",
    b"sparrow.store/173",
    b"sparrow.store/174",
    b"sparrow.store/175",
];
const FALLBACK_INTAKE_ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/176",
    b"sparrow.store/177",
    b"sparrow.store/178",
    b"sparrow.store/179",
    b"sparrow.store/180",
    b"sparrow.store/181",
    b"sparrow.store/182",
    b"sparrow.store/183",
    b"sparrow.store/184",
    b"sparrow.store/185",
    b"sparrow.store/186",
];

/// Families the event fallback fence lifts, in the order of their lifted counterparts.
const LIFTED: [[&[u8]; 11]; 5] = [TRACING, LOG_TIME, ARCHIVE, INTAKE, INTAKE_ARCHIVE];
const FALLBACK: [[&[u8]; 11]; 5] = [
    FALLBACK_TRACING,
    FALLBACK_LOG_TIME,
    FALLBACK_ARCHIVE,
    FALLBACK_INTAKE,
    FALLBACK_INTAKE_ARCHIVE,
];

// Owner-to-graph index: older binaries cannot publish or retire a graph declaration
// without its `rdf:o:` mirror. Each family lifts one event fallback family by 55,
// keeping its log time, archive and intake switches.
const OWNER_GRAPH_TRACING: [&[u8]; 11] = [
    b"sparrow.store/187",
    b"sparrow.store/188",
    b"sparrow.store/189",
    b"sparrow.store/190",
    b"sparrow.store/191",
    b"sparrow.store/192",
    b"sparrow.store/193",
    b"sparrow.store/194",
    b"sparrow.store/195",
    b"sparrow.store/196",
    b"sparrow.store/197",
];
const OWNER_GRAPH_LOG_TIME: [&[u8]; 11] = [
    b"sparrow.store/198",
    b"sparrow.store/199",
    b"sparrow.store/200",
    b"sparrow.store/201",
    b"sparrow.store/202",
    b"sparrow.store/203",
    b"sparrow.store/204",
    b"sparrow.store/205",
    b"sparrow.store/206",
    b"sparrow.store/207",
    b"sparrow.store/208",
];
const OWNER_GRAPH_ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/209",
    b"sparrow.store/210",
    b"sparrow.store/211",
    b"sparrow.store/212",
    b"sparrow.store/213",
    b"sparrow.store/214",
    b"sparrow.store/215",
    b"sparrow.store/216",
    b"sparrow.store/217",
    b"sparrow.store/218",
    b"sparrow.store/219",
];
const OWNER_GRAPH_INTAKE: [&[u8]; 11] = [
    b"sparrow.store/220",
    b"sparrow.store/221",
    b"sparrow.store/222",
    b"sparrow.store/223",
    b"sparrow.store/224",
    b"sparrow.store/225",
    b"sparrow.store/226",
    b"sparrow.store/227",
    b"sparrow.store/228",
    b"sparrow.store/229",
    b"sparrow.store/230",
];
const OWNER_GRAPH_INTAKE_ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/231",
    b"sparrow.store/232",
    b"sparrow.store/233",
    b"sparrow.store/234",
    b"sparrow.store/235",
    b"sparrow.store/236",
    b"sparrow.store/237",
    b"sparrow.store/238",
    b"sparrow.store/239",
    b"sparrow.store/240",
    b"sparrow.store/241",
];

/// Families the owner graph fence lifts, in the order of their lifted counterparts.
const OWNER_GRAPH: [[&[u8]; 11]; 5] = [
    OWNER_GRAPH_TRACING,
    OWNER_GRAPH_LOG_TIME,
    OWNER_GRAPH_ARCHIVE,
    OWNER_GRAPH_INTAKE,
    OWNER_GRAPH_INTAKE_ARCHIVE,
];

// Per-source counters and the per-source artifact index (#14): older binaries cannot change
// a record or artifact entry without its `source-summary:`, `artifact-retained:` and
// `source-artifact:` rows. Each family lifts one owner graph family by 55, keeping its log
// time, archive and intake switches.
const SOURCE_SUMMARY_TRACING: [&[u8]; 11] = [
    b"sparrow.store/242",
    b"sparrow.store/243",
    b"sparrow.store/244",
    b"sparrow.store/245",
    b"sparrow.store/246",
    b"sparrow.store/247",
    b"sparrow.store/248",
    b"sparrow.store/249",
    b"sparrow.store/250",
    b"sparrow.store/251",
    b"sparrow.store/252",
];
const SOURCE_SUMMARY_LOG_TIME: [&[u8]; 11] = [
    b"sparrow.store/253",
    b"sparrow.store/254",
    b"sparrow.store/255",
    b"sparrow.store/256",
    b"sparrow.store/257",
    b"sparrow.store/258",
    b"sparrow.store/259",
    b"sparrow.store/260",
    b"sparrow.store/261",
    b"sparrow.store/262",
    b"sparrow.store/263",
];
const SOURCE_SUMMARY_ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/264",
    b"sparrow.store/265",
    b"sparrow.store/266",
    b"sparrow.store/267",
    b"sparrow.store/268",
    b"sparrow.store/269",
    b"sparrow.store/270",
    b"sparrow.store/271",
    b"sparrow.store/272",
    b"sparrow.store/273",
    b"sparrow.store/274",
];
const SOURCE_SUMMARY_INTAKE: [&[u8]; 11] = [
    b"sparrow.store/275",
    b"sparrow.store/276",
    b"sparrow.store/277",
    b"sparrow.store/278",
    b"sparrow.store/279",
    b"sparrow.store/280",
    b"sparrow.store/281",
    b"sparrow.store/282",
    b"sparrow.store/283",
    b"sparrow.store/284",
    b"sparrow.store/285",
];
const SOURCE_SUMMARY_INTAKE_ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/286",
    b"sparrow.store/287",
    b"sparrow.store/288",
    b"sparrow.store/289",
    b"sparrow.store/290",
    b"sparrow.store/291",
    b"sparrow.store/292",
    b"sparrow.store/293",
    b"sparrow.store/294",
    b"sparrow.store/295",
    b"sparrow.store/296",
];

/// Families the source summary fence lifts, in the order of their lifted counterparts.
const SOURCE_SUMMARY: [[&[u8]; 11]; 5] = [
    SOURCE_SUMMARY_TRACING,
    SOURCE_SUMMARY_LOG_TIME,
    SOURCE_SUMMARY_ARCHIVE,
    SOURCE_SUMMARY_INTAKE,
    SOURCE_SUMMARY_INTAKE_ARCHIVE,
];

// Store counters (#146): older binaries cannot change a counted key or move a migration
// checkpoint without the `store-counter:` rows. Each family lifts one source summary family
// by 55, keeping its log time, archive and intake switches.
const STORE_COUNTERS_TRACING: [&[u8]; 11] = [
    b"sparrow.store/297",
    b"sparrow.store/298",
    b"sparrow.store/299",
    b"sparrow.store/300",
    b"sparrow.store/301",
    b"sparrow.store/302",
    b"sparrow.store/303",
    b"sparrow.store/304",
    b"sparrow.store/305",
    b"sparrow.store/306",
    b"sparrow.store/307",
];
const STORE_COUNTERS_LOG_TIME: [&[u8]; 11] = [
    b"sparrow.store/308",
    b"sparrow.store/309",
    b"sparrow.store/310",
    b"sparrow.store/311",
    b"sparrow.store/312",
    b"sparrow.store/313",
    b"sparrow.store/314",
    b"sparrow.store/315",
    b"sparrow.store/316",
    b"sparrow.store/317",
    b"sparrow.store/318",
];
const STORE_COUNTERS_ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/319",
    b"sparrow.store/320",
    b"sparrow.store/321",
    b"sparrow.store/322",
    b"sparrow.store/323",
    b"sparrow.store/324",
    b"sparrow.store/325",
    b"sparrow.store/326",
    b"sparrow.store/327",
    b"sparrow.store/328",
    b"sparrow.store/329",
];
const STORE_COUNTERS_INTAKE: [&[u8]; 11] = [
    b"sparrow.store/330",
    b"sparrow.store/331",
    b"sparrow.store/332",
    b"sparrow.store/333",
    b"sparrow.store/334",
    b"sparrow.store/335",
    b"sparrow.store/336",
    b"sparrow.store/337",
    b"sparrow.store/338",
    b"sparrow.store/339",
    b"sparrow.store/340",
];
const STORE_COUNTERS_INTAKE_ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/341",
    b"sparrow.store/342",
    b"sparrow.store/343",
    b"sparrow.store/344",
    b"sparrow.store/345",
    b"sparrow.store/346",
    b"sparrow.store/347",
    b"sparrow.store/348",
    b"sparrow.store/349",
    b"sparrow.store/350",
    b"sparrow.store/351",
];

/// Families the store counter fence lifts, in the order of their lifted counterparts.
const STORE_COUNTERS: [[&[u8]; 11]; 5] = [
    STORE_COUNTERS_TRACING,
    STORE_COUNTERS_LOG_TIME,
    STORE_COUNTERS_ARCHIVE,
    STORE_COUNTERS_INTAKE,
    STORE_COUNTERS_INTAKE_ARCHIVE,
];

// Payload id index (#323): older binaries cannot write or delete a `payload-id:` key with the
// artifact entry it follows. Each family lifts one store counter family by 55, keeping its
// log time, archive and intake switches.
const PAYLOAD_INDEX_TRACING: [&[u8]; 11] = [
    b"sparrow.store/352",
    b"sparrow.store/353",
    b"sparrow.store/354",
    b"sparrow.store/355",
    b"sparrow.store/356",
    b"sparrow.store/357",
    b"sparrow.store/358",
    b"sparrow.store/359",
    b"sparrow.store/360",
    b"sparrow.store/361",
    b"sparrow.store/362",
];
const PAYLOAD_INDEX_LOG_TIME: [&[u8]; 11] = [
    b"sparrow.store/363",
    b"sparrow.store/364",
    b"sparrow.store/365",
    b"sparrow.store/366",
    b"sparrow.store/367",
    b"sparrow.store/368",
    b"sparrow.store/369",
    b"sparrow.store/370",
    b"sparrow.store/371",
    b"sparrow.store/372",
    b"sparrow.store/373",
];
const PAYLOAD_INDEX_ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/374",
    b"sparrow.store/375",
    b"sparrow.store/376",
    b"sparrow.store/377",
    b"sparrow.store/378",
    b"sparrow.store/379",
    b"sparrow.store/380",
    b"sparrow.store/381",
    b"sparrow.store/382",
    b"sparrow.store/383",
    b"sparrow.store/384",
];
const PAYLOAD_INDEX_INTAKE: [&[u8]; 11] = [
    b"sparrow.store/385",
    b"sparrow.store/386",
    b"sparrow.store/387",
    b"sparrow.store/388",
    b"sparrow.store/389",
    b"sparrow.store/390",
    b"sparrow.store/391",
    b"sparrow.store/392",
    b"sparrow.store/393",
    b"sparrow.store/394",
    b"sparrow.store/395",
];
const PAYLOAD_INDEX_INTAKE_ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/396",
    b"sparrow.store/397",
    b"sparrow.store/398",
    b"sparrow.store/399",
    b"sparrow.store/400",
    b"sparrow.store/401",
    b"sparrow.store/402",
    b"sparrow.store/403",
    b"sparrow.store/404",
    b"sparrow.store/405",
    b"sparrow.store/406",
];

/// Families the payload index fence lifts, in the order of their lifted counterparts.
const PAYLOAD_INDEX: [[&[u8]; 11]; 5] = [
    PAYLOAD_INDEX_TRACING,
    PAYLOAD_INDEX_LOG_TIME,
    PAYLOAD_INDEX_ARCHIVE,
    PAYLOAD_INDEX_INTAKE,
    PAYLOAD_INDEX_INTAKE_ARCHIVE,
];

fn position(families: &[[&'static [u8]; 11]; 5], format: &[u8]) -> Option<(usize, usize)> {
    families
        .iter()
        .enumerate()
        .find_map(|(family, identities)| {
            identities
                .iter()
                .position(|identity| *identity == format)
                .map(|index| (family, index))
        })
}

// Sealed producer replay requires every writer that retires an event receipt to
// advance the durable GC watermark. Preserve the five payload-index families.
const SEALED: [[&[u8]; 11]; 5] = [
    [
        b"sparrow.store/407",
        b"sparrow.store/408",
        b"sparrow.store/409",
        b"sparrow.store/410",
        b"sparrow.store/411",
        b"sparrow.store/412",
        b"sparrow.store/413",
        b"sparrow.store/414",
        b"sparrow.store/415",
        b"sparrow.store/416",
        b"sparrow.store/417",
    ],
    [
        b"sparrow.store/418",
        b"sparrow.store/419",
        b"sparrow.store/420",
        b"sparrow.store/421",
        b"sparrow.store/422",
        b"sparrow.store/423",
        b"sparrow.store/424",
        b"sparrow.store/425",
        b"sparrow.store/426",
        b"sparrow.store/427",
        b"sparrow.store/428",
    ],
    [
        b"sparrow.store/429",
        b"sparrow.store/430",
        b"sparrow.store/431",
        b"sparrow.store/432",
        b"sparrow.store/433",
        b"sparrow.store/434",
        b"sparrow.store/435",
        b"sparrow.store/436",
        b"sparrow.store/437",
        b"sparrow.store/438",
        b"sparrow.store/439",
    ],
    [
        b"sparrow.store/440",
        b"sparrow.store/441",
        b"sparrow.store/442",
        b"sparrow.store/443",
        b"sparrow.store/444",
        b"sparrow.store/445",
        b"sparrow.store/446",
        b"sparrow.store/447",
        b"sparrow.store/448",
        b"sparrow.store/449",
        b"sparrow.store/450",
    ],
    [
        b"sparrow.store/451",
        b"sparrow.store/452",
        b"sparrow.store/453",
        b"sparrow.store/454",
        b"sparrow.store/455",
        b"sparrow.store/456",
        b"sparrow.store/457",
        b"sparrow.store/458",
        b"sparrow.store/459",
        b"sparrow.store/460",
        b"sparrow.store/461",
    ],
];

// The expanded source summary rows require a reader and writer that know their byte totals.
const SOURCE_BYTES: [[&[u8]; 11]; 5] = [
    [
        b"sparrow.store/462",
        b"sparrow.store/463",
        b"sparrow.store/464",
        b"sparrow.store/465",
        b"sparrow.store/466",
        b"sparrow.store/467",
        b"sparrow.store/468",
        b"sparrow.store/469",
        b"sparrow.store/470",
        b"sparrow.store/471",
        b"sparrow.store/472",
    ],
    [
        b"sparrow.store/473",
        b"sparrow.store/474",
        b"sparrow.store/475",
        b"sparrow.store/476",
        b"sparrow.store/477",
        b"sparrow.store/478",
        b"sparrow.store/479",
        b"sparrow.store/480",
        b"sparrow.store/481",
        b"sparrow.store/482",
        b"sparrow.store/483",
    ],
    [
        b"sparrow.store/484",
        b"sparrow.store/485",
        b"sparrow.store/486",
        b"sparrow.store/487",
        b"sparrow.store/488",
        b"sparrow.store/489",
        b"sparrow.store/490",
        b"sparrow.store/491",
        b"sparrow.store/492",
        b"sparrow.store/493",
        b"sparrow.store/494",
    ],
    [
        b"sparrow.store/495",
        b"sparrow.store/496",
        b"sparrow.store/497",
        b"sparrow.store/498",
        b"sparrow.store/499",
        b"sparrow.store/500",
        b"sparrow.store/501",
        b"sparrow.store/502",
        b"sparrow.store/503",
        b"sparrow.store/504",
        b"sparrow.store/505",
    ],
    [
        b"sparrow.store/506",
        b"sparrow.store/507",
        b"sparrow.store/508",
        b"sparrow.store/509",
        b"sparrow.store/510",
        b"sparrow.store/511",
        b"sparrow.store/512",
        b"sparrow.store/513",
        b"sparrow.store/514",
        b"sparrow.store/515",
        b"sparrow.store/516",
    ],
];
const SEALED_SOURCE_BYTES: [[&[u8]; 11]; 5] = [
    [
        b"sparrow.store/517",
        b"sparrow.store/518",
        b"sparrow.store/519",
        b"sparrow.store/520",
        b"sparrow.store/521",
        b"sparrow.store/522",
        b"sparrow.store/523",
        b"sparrow.store/524",
        b"sparrow.store/525",
        b"sparrow.store/526",
        b"sparrow.store/527",
    ],
    [
        b"sparrow.store/528",
        b"sparrow.store/529",
        b"sparrow.store/530",
        b"sparrow.store/531",
        b"sparrow.store/532",
        b"sparrow.store/533",
        b"sparrow.store/534",
        b"sparrow.store/535",
        b"sparrow.store/536",
        b"sparrow.store/537",
        b"sparrow.store/538",
    ],
    [
        b"sparrow.store/539",
        b"sparrow.store/540",
        b"sparrow.store/541",
        b"sparrow.store/542",
        b"sparrow.store/543",
        b"sparrow.store/544",
        b"sparrow.store/545",
        b"sparrow.store/546",
        b"sparrow.store/547",
        b"sparrow.store/548",
        b"sparrow.store/549",
    ],
    [
        b"sparrow.store/550",
        b"sparrow.store/551",
        b"sparrow.store/552",
        b"sparrow.store/553",
        b"sparrow.store/554",
        b"sparrow.store/555",
        b"sparrow.store/556",
        b"sparrow.store/557",
        b"sparrow.store/558",
        b"sparrow.store/559",
        b"sparrow.store/560",
    ],
    [
        b"sparrow.store/561",
        b"sparrow.store/562",
        b"sparrow.store/563",
        b"sparrow.store/564",
        b"sparrow.store/565",
        b"sparrow.store/566",
        b"sparrow.store/567",
        b"sparrow.store/568",
        b"sparrow.store/569",
        b"sparrow.store/570",
        b"sparrow.store/571",
    ],
];

// Per-owner payload summaries: older binaries cannot change an artifact or record entry
// without its `owner-summary:` row, so they refuse every identity below.
const OWNER_SUMMARY: [[&[u8]; 11]; 5] = [
    [
        b"sparrow.store/572",
        b"sparrow.store/573",
        b"sparrow.store/574",
        b"sparrow.store/575",
        b"sparrow.store/576",
        b"sparrow.store/577",
        b"sparrow.store/578",
        b"sparrow.store/579",
        b"sparrow.store/580",
        b"sparrow.store/581",
        b"sparrow.store/582",
    ],
    [
        b"sparrow.store/583",
        b"sparrow.store/584",
        b"sparrow.store/585",
        b"sparrow.store/586",
        b"sparrow.store/587",
        b"sparrow.store/588",
        b"sparrow.store/589",
        b"sparrow.store/590",
        b"sparrow.store/591",
        b"sparrow.store/592",
        b"sparrow.store/593",
    ],
    [
        b"sparrow.store/594",
        b"sparrow.store/595",
        b"sparrow.store/596",
        b"sparrow.store/597",
        b"sparrow.store/598",
        b"sparrow.store/599",
        b"sparrow.store/600",
        b"sparrow.store/601",
        b"sparrow.store/602",
        b"sparrow.store/603",
        b"sparrow.store/604",
    ],
    [
        b"sparrow.store/605",
        b"sparrow.store/606",
        b"sparrow.store/607",
        b"sparrow.store/608",
        b"sparrow.store/609",
        b"sparrow.store/610",
        b"sparrow.store/611",
        b"sparrow.store/612",
        b"sparrow.store/613",
        b"sparrow.store/614",
        b"sparrow.store/615",
    ],
    [
        b"sparrow.store/616",
        b"sparrow.store/617",
        b"sparrow.store/618",
        b"sparrow.store/619",
        b"sparrow.store/620",
        b"sparrow.store/621",
        b"sparrow.store/622",
        b"sparrow.store/623",
        b"sparrow.store/624",
        b"sparrow.store/625",
        b"sparrow.store/626",
    ],
];
// The same fence over a store that also requires sealed producer replay.
const SEALED_OWNER_SUMMARY: [[&[u8]; 11]; 5] = [
    [
        b"sparrow.store/627",
        b"sparrow.store/628",
        b"sparrow.store/629",
        b"sparrow.store/630",
        b"sparrow.store/631",
        b"sparrow.store/632",
        b"sparrow.store/633",
        b"sparrow.store/634",
        b"sparrow.store/635",
        b"sparrow.store/636",
        b"sparrow.store/637",
    ],
    [
        b"sparrow.store/638",
        b"sparrow.store/639",
        b"sparrow.store/640",
        b"sparrow.store/641",
        b"sparrow.store/642",
        b"sparrow.store/643",
        b"sparrow.store/644",
        b"sparrow.store/645",
        b"sparrow.store/646",
        b"sparrow.store/647",
        b"sparrow.store/648",
    ],
    [
        b"sparrow.store/649",
        b"sparrow.store/650",
        b"sparrow.store/651",
        b"sparrow.store/652",
        b"sparrow.store/653",
        b"sparrow.store/654",
        b"sparrow.store/655",
        b"sparrow.store/656",
        b"sparrow.store/657",
        b"sparrow.store/658",
        b"sparrow.store/659",
    ],
    [
        b"sparrow.store/660",
        b"sparrow.store/661",
        b"sparrow.store/662",
        b"sparrow.store/663",
        b"sparrow.store/664",
        b"sparrow.store/665",
        b"sparrow.store/666",
        b"sparrow.store/667",
        b"sparrow.store/668",
        b"sparrow.store/669",
        b"sparrow.store/670",
    ],
    [
        b"sparrow.store/671",
        b"sparrow.store/672",
        b"sparrow.store/673",
        b"sparrow.store/674",
        b"sparrow.store/675",
        b"sparrow.store/676",
        b"sparrow.store/677",
        b"sparrow.store/678",
        b"sparrow.store/679",
        b"sparrow.store/680",
        b"sparrow.store/681",
    ],
];

pub(super) fn has_source_bytes(format: &[u8]) -> bool {
    position(&SOURCE_BYTES, format).is_some()
        || position(&SEALED_SOURCE_BYTES, format).is_some()
        || has_owner_summary(format)
}

/// Whether `format` fences the per-owner payload summary rows.
pub(super) fn has_owner_summary(format: &[u8]) -> bool {
    owner_summary(format).is_some()
}

fn owner_summary(format: &[u8]) -> Option<&'static [u8]> {
    position(&SEALED_OWNER_SUMMARY, format)
        .map(|(family, index)| SEALED_OWNER_SUMMARY[family][index])
        .or_else(|| {
            position(&OWNER_SUMMARY, format).map(|(family, index)| OWNER_SUMMARY[family][index])
        })
}

/// Fence a current source bytes family for the per-owner payload summary, keeping the
/// sealed replay switch; idempotent.
pub(super) fn enable_owner_summary(format: &[u8]) -> Result<&'static [u8], Error> {
    if let Some(identity) = owner_summary(format) {
        return Ok(identity);
    }
    position(&SOURCE_BYTES, format)
        .map(|(family, index)| OWNER_SUMMARY[family][index])
        .or_else(|| {
            position(&SEALED_SOURCE_BYTES, format)
                .map(|(family, index)| SEALED_OWNER_SUMMARY[family][index])
        })
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "the owner summary requires a source bytes authority family",
            )
        })
}

/// The source bytes identity an owner summary family lifts; other formats are unchanged.
fn before_owner_summary(format: &[u8]) -> &[u8] {
    if let Some((family, index)) = position(&OWNER_SUMMARY, format) {
        return SOURCE_BYTES[family][index];
    }
    position(&SEALED_OWNER_SUMMARY, format)
        .map_or(format, |(family, index)| SEALED_SOURCE_BYTES[family][index])
}

pub(super) fn enable_source_bytes(format: &[u8]) -> Result<&'static [u8], Error> {
    if let Some(identity) = owner_summary(format) {
        return Ok(identity);
    }
    position(&SOURCE_BYTES, format)
        .or_else(|| position(&PAYLOAD_INDEX, format))
        .map(|(family, index)| SOURCE_BYTES[family][index])
        .or_else(|| {
            position(&SEALED_SOURCE_BYTES, format)
                .or_else(|| position(&SEALED, format))
                .map(|(family, index)| SEALED_SOURCE_BYTES[family][index])
        })
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "source bytes require a payload-index authority family",
            )
        })
}

fn before_source_bytes(format: &[u8]) -> &[u8] {
    let format = before_owner_summary(format);
    if let Some((family, index)) = position(&SOURCE_BYTES, format) {
        return PAYLOAD_INDEX[family][index];
    }
    position(&SEALED_SOURCE_BYTES, format).map_or(format, |(family, index)| SEALED[family][index])
}

pub(super) const RETRY_GC: &str = "producer:sealed-receipt-gc";

fn sealed(format: &[u8]) -> Option<&'static [u8]> {
    position(&SEALED_OWNER_SUMMARY, format)
        .map(|(family, index)| SEALED_OWNER_SUMMARY[family][index])
        .or_else(|| {
            position(&SEALED_SOURCE_BYTES, format)
                .map(|(family, index)| SEALED_SOURCE_BYTES[family][index])
        })
        .or_else(|| position(&SEALED, format).map(|(family, index)| SEALED[family][index]))
}

fn payload_index(format: &[u8]) -> Option<&'static [u8]> {
    position(&OWNER_SUMMARY, format)
        .map(|(family, index)| OWNER_SUMMARY[family][index])
        .or_else(|| {
            position(&SOURCE_BYTES, format).map(|(family, index)| SOURCE_BYTES[family][index])
        })
        .or_else(|| {
            position(&PAYLOAD_INDEX, format).map(|(family, index)| PAYLOAD_INDEX[family][index])
        })
}

fn enable_sealed(format: &[u8]) -> Result<&'static [u8], Error> {
    if let Some((family, index)) = position(&OWNER_SUMMARY, format) {
        return Ok(SEALED_OWNER_SUMMARY[family][index]);
    }
    if let Some((family, index)) = position(&SOURCE_BYTES, format) {
        return Ok(SEALED_SOURCE_BYTES[family][index]);
    }
    if let Some(identity) = sealed(format) {
        return Ok(identity);
    }
    position(&PAYLOAD_INDEX, format)
        .map(|(family, i)| SEALED[family][i])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "sealed events require a payload-index authority family",
            )
        })
}

pub(super) fn retry_watermark(db: &DB) -> Result<Option<u64>, Error> {
    let format = db
        .get("store_format")?
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "missing store format"))?;
    match (sealed(&format), db.get(RETRY_GC)?) {
        (None, None) => Ok(None),
        (Some(_), Some(bytes)) if bytes.len() == 8 => Ok(Some(u64::from_be_bytes(
            bytes
                .as_slice()
                .try_into()
                .map_err(|_| Error::new("STORAGE_FAILURE", "invalid sealed receipt watermark"))?,
        ))),
        _ => Err(Error::new(
            "STORAGE_FAILURE",
            "sealed receipt watermark contradicts writer format",
        )),
    }
}

pub(super) fn activate_sealed(db: &DB) -> Result<(), Error> {
    let format = db
        .get("store_format")?
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "missing store format"))?;
    let mut batch = WriteBatch::default();
    batch.put("store_format", enable_sealed(&format)?);
    batch.put(RETRY_GC, 0u64.to_be_bytes());
    super::catalog_upgrade::commit(db, batch)
}

/// The pre-fallback identity a store-counter, source-summary, owner-graph or fallback-fenced
/// format lifts;
/// other formats are unchanged.
fn lower(format: &[u8]) -> &[u8] {
    let format = before_source_bytes(format);
    let format = position(&SEALED, format).map_or(format, |(family, i)| PAYLOAD_INDEX[family][i]);
    let format = position(&PAYLOAD_INDEX, format)
        .map_or(format, |(family, index)| STORE_COUNTERS[family][index]);
    let format = position(&STORE_COUNTERS, format)
        .map_or(format, |(family, index)| SOURCE_SUMMARY[family][index]);
    let format = position(&SOURCE_SUMMARY, format)
        .map_or(format, |(family, index)| OWNER_GRAPH[family][index]);
    let format =
        position(&OWNER_GRAPH, format).map_or(format, |(family, index)| FALLBACK[family][index]);
    position(&FALLBACK, format).map_or(format, |(family, index)| LIFTED[family][index])
}

/// Re-apply the fallback, owner graph, source summary and store counter fences to a transition computed on
/// the lowered identity.
fn relift<'a>(current: &[u8], next: &'a [u8]) -> Result<&'a [u8], Error> {
    let next = if has_event_fallback(current) {
        enable_event_fallback(next)?
    } else {
        next
    };
    let next = if has_owner_graphs(current) {
        enable_owner_graphs(next)?
    } else {
        next
    };
    let next = if has_source_summary(current) {
        enable_source_summary(next)?
    } else {
        next
    };
    let next = if has_store_counters(current) {
        enable_store_counters(next)?
    } else {
        next
    };
    let next = if sealed(current).is_some() {
        enable_sealed(enable_payload_index(next)?)
    } else if has_payload_index(current) {
        enable_payload_index(next)
    } else {
        Ok(next)
    }?;
    let next = if has_source_bytes(current) {
        enable_source_bytes(next)
    } else {
        Ok(next)
    }?;
    if has_owner_summary(current) {
        enable_owner_summary(next)
    } else {
        Ok(next)
    }
}

pub(super) fn has_event_fallback(format: &[u8]) -> bool {
    position(&FALLBACK, format).is_some() || has_owner_graphs(format)
}

pub(super) fn has_owner_graphs(format: &[u8]) -> bool {
    position(&OWNER_GRAPH, format).is_some() || has_source_summary(format)
}

/// The owner graph identity a source summary or store counter family lifts; other formats
/// are unchanged.
#[cfg(feature = "test-support")]
pub(super) fn lower_source_summary(format: &[u8]) -> &[u8] {
    let format = lower_store_counters(format);
    position(&SOURCE_SUMMARY, format).map_or(format, |(family, index)| OWNER_GRAPH[family][index])
}

/// The source summary identity a store counter family lifts; other formats are unchanged.
#[cfg(feature = "test-support")]
pub(super) fn lower_store_counters(format: &[u8]) -> &[u8] {
    let format = lower_payload_index(format);
    position(&STORE_COUNTERS, format)
        .map_or(format, |(family, index)| SOURCE_SUMMARY[family][index])
}

/// The source bytes identity an owner summary family lifts; other formats are unchanged.
#[cfg(feature = "test-support")]
pub(super) fn lower_owner_summary(format: &[u8]) -> &[u8] {
    before_owner_summary(format)
}

/// The store counter identity a payload index family lifts; other formats are unchanged.
#[cfg(feature = "test-support")]
pub(super) fn lower_payload_index(format: &[u8]) -> &[u8] {
    let format = before_source_bytes(format);
    let format = position(&SEALED, format).map_or(format, |(family, i)| PAYLOAD_INDEX[family][i]);
    position(&PAYLOAD_INDEX, format).map_or(format, |(family, index)| STORE_COUNTERS[family][index])
}

pub(super) fn has_source_summary(format: &[u8]) -> bool {
    position(&SOURCE_SUMMARY, format).is_some() || has_store_counters(format)
}

pub(super) fn has_store_counters(format: &[u8]) -> bool {
    position(&STORE_COUNTERS, format).is_some() || has_payload_index(format)
}

pub(super) fn has_payload_index(format: &[u8]) -> bool {
    payload_index(format).is_some() || sealed(format).is_some()
}

/// Fence a current store counter family for the payload id index (#323); idempotent.
pub(super) fn enable_payload_index(format: &[u8]) -> Result<&'static [u8], Error> {
    if let Some(identity) = sealed(format) {
        return Ok(identity);
    }
    payload_index(format)
        .or_else(|| {
            position(&STORE_COUNTERS, format).map(|(family, index)| PAYLOAD_INDEX[family][index])
        })
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "the payload id index requires a store counter authority family",
            )
        })
}

/// Fence a current source summary family for the store counters; idempotent.
pub(super) fn enable_store_counters(format: &[u8]) -> Result<&'static [u8], Error> {
    if let Some(identity) = sealed(format) {
        return Ok(identity);
    }
    if let Some(identity) = payload_index(format) {
        return Ok(identity);
    }
    position(&STORE_COUNTERS, format)
        .or_else(|| position(&SOURCE_SUMMARY, format))
        .map(|(family, index)| STORE_COUNTERS[family][index])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "store counters require a source summary authority family",
            )
        })
}

/// Fence a current owner graph family for the per-source counters and artifact index;
/// idempotent.
pub(super) fn enable_source_summary(format: &[u8]) -> Result<&'static [u8], Error> {
    if let Some(identity) = sealed(format) {
        return Ok(identity);
    }
    if let Some(identity) = payload_index(format) {
        return Ok(identity);
    }
    if let Some((family, index)) = position(&STORE_COUNTERS, format) {
        return Ok(STORE_COUNTERS[family][index]);
    }
    position(&SOURCE_SUMMARY, format)
        .or_else(|| position(&OWNER_GRAPH, format))
        .map(|(family, index)| SOURCE_SUMMARY[family][index])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "per-source counters require an owner graph authority family",
            )
        })
}

/// Fence a current event fallback family for the owner-to-graph index; idempotent.
pub(super) fn enable_owner_graphs(format: &[u8]) -> Result<&'static [u8], Error> {
    if let Some(identity) = sealed(format) {
        return Ok(identity);
    }
    if let Some(identity) = payload_index(format) {
        return Ok(identity);
    }
    if let Some((family, index)) = position(&STORE_COUNTERS, format) {
        return Ok(STORE_COUNTERS[family][index]);
    }
    if let Some((family, index)) = position(&SOURCE_SUMMARY, format) {
        return Ok(SOURCE_SUMMARY[family][index]);
    }
    position(&OWNER_GRAPH, format)
        .or_else(|| position(&FALLBACK, format))
        .map(|(family, index)| OWNER_GRAPH[family][index])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "owner graph index requires an event fallback authority family",
            )
        })
}

/// Fence a current tracing-or-later family for receipt-time fallback RDF; idempotent.
pub(super) fn enable_event_fallback(format: &[u8]) -> Result<&'static [u8], Error> {
    if let Some(identity) = sealed(format) {
        return Ok(identity);
    }
    if let Some(identity) = payload_index(format) {
        return Ok(identity);
    }
    if let Some((family, index)) = position(&STORE_COUNTERS, format) {
        return Ok(STORE_COUNTERS[family][index]);
    }
    if let Some((family, index)) = position(&SOURCE_SUMMARY, format) {
        return Ok(SOURCE_SUMMARY[family][index]);
    }
    if let Some((family, index)) = position(&OWNER_GRAPH, format) {
        return Ok(OWNER_GRAPH[family][index]);
    }
    if let Some((family, index)) = position(&FALLBACK, format) {
        return Ok(FALLBACK[family][index]);
    }
    position(&LIFTED, format)
        .map(|(family, index)| FALLBACK[family][index])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "event fallback RDF requires a tracing or later authority family",
            )
        })
}

/// Every family in order; a position modulo `BASE.len()` names the original authority.
fn families() -> impl Iterator<Item = &'static &'static [u8]> {
    BASE.iter()
        .chain(RDF.iter())
        .chain(ALL_RDF.iter())
        .chain(PROVENANCE.iter())
        .chain(TRACING.iter())
        .chain(LOG_TIME.iter())
        .chain(ARCHIVE.iter())
        .chain(INTAKE.iter())
        .chain(INTAKE_ARCHIVE.iter())
        .chain(FALLBACK.iter().flatten())
        .chain(OWNER_GRAPH.iter().flatten())
        .chain(SOURCE_SUMMARY.iter().flatten())
        .chain(STORE_COUNTERS.iter().flatten())
        .chain(PAYLOAD_INDEX.iter().flatten())
        .chain(SEALED.iter().flatten())
        .chain(SOURCE_BYTES.iter().flatten())
        .chain(SEALED_SOURCE_BYTES.iter().flatten())
        .chain(OWNER_SUMMARY.iter().flatten())
        .chain(SEALED_OWNER_SUMMARY.iter().flatten())
}

fn base_index(format: &[u8], purpose: &str) -> Result<usize, Error> {
    families()
        .position(|identity| *identity == format)
        .map(|index| index % BASE.len())
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                format!("{purpose} requires a current authority family"),
            )
        })
}

pub(super) fn has_intake(format: &[u8]) -> bool {
    let format = lower(format);
    INTAKE.contains(&format) || INTAKE_ARCHIVE.contains(&format)
}

/// Fence any current authority family for intake provenance, keeping its archive switch.
pub(super) fn enable_intake(format: &[u8]) -> Result<&'static [u8], Error> {
    let index = base_index(format, "intake provenance fence")?;
    relift(
        format,
        if has_archive(format) {
            INTAKE_ARCHIVE[index]
        } else {
            INTAKE[index]
        },
    )
}

pub(super) fn has_archive(format: &[u8]) -> bool {
    let format = lower(format);
    ARCHIVE.contains(&format) || INTAKE_ARCHIVE.contains(&format)
}

/// Fence any current authority family for archive-before-delete; idempotent.
pub(super) fn enable_archive(format: &[u8]) -> Result<&'static [u8], Error> {
    let index = base_index(format, "archive fence")?;
    relift(
        format,
        if has_intake(format) {
            INTAKE_ARCHIVE[index]
        } else {
            ARCHIVE[index]
        },
    )
}

pub(super) fn has_log_time(format: &[u8]) -> bool {
    let format = lower(format);
    LOG_TIME.contains(&format) || has_archive(format) || has_intake(format)
}

pub(super) fn enable_log_time(format: &[u8]) -> Result<&'static [u8], Error> {
    relift(format, enable_log_time_lowered(lower(format))?)
}

fn enable_log_time_lowered(format: &[u8]) -> Result<&'static [u8], Error> {
    // The archive and intake families already include the log time law; never lower them.
    if let Some(identity) = ARCHIVE
        .iter()
        .chain(INTAKE.iter())
        .chain(INTAKE_ARCHIVE.iter())
        .find(|identity| **identity == format)
    {
        return Ok(identity);
    }
    BASE.iter()
        .chain(RDF.iter())
        .chain(ALL_RDF.iter())
        .chain(PROVENANCE.iter())
        .chain(TRACING.iter())
        .chain(LOG_TIME.iter())
        .position(|identity| *identity == format)
        .map(|index| LOG_TIME[index % BASE.len()])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "log time mapping requires a current authority family",
            )
        })
}

pub(super) fn has_tracing(format: &[u8]) -> bool {
    let format = lower(format);
    TRACING.contains(&format) || has_log_time(format)
}

pub(super) fn enable_tracing(format: &[u8]) -> Result<&'static [u8], Error> {
    BASE.iter()
        .chain(RDF.iter())
        .chain(ALL_RDF.iter())
        .chain(PROVENANCE.iter())
        .chain(TRACING.iter())
        .position(|identity| *identity == format)
        .map(|index| TRACING[index % BASE.len()])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "tracing RDF requires a current authority family",
            )
        })
}

pub(super) fn has_provenance(format: &[u8]) -> bool {
    PROVENANCE.contains(&format) || has_tracing(format)
}

pub(super) fn enable_provenance(format: &[u8]) -> Result<&'static [u8], Error> {
    BASE.iter()
        .chain(RDF.iter())
        .chain(ALL_RDF.iter())
        .chain(PROVENANCE.iter())
        .position(|identity| *identity == format)
        .map(|index| PROVENANCE[index % BASE.len()])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "provenance RDF requires a current authority family",
            )
        })
}

pub(super) fn has_all_artifact_rdf(format: &[u8]) -> bool {
    ALL_RDF.contains(&format) || has_provenance(format)
}

pub(super) fn has_artifact_rdf(format: &[u8]) -> bool {
    RDF.contains(&format) || has_all_artifact_rdf(format)
}

/// Authority checks use their original family. Public discovery uses the raw identity.
pub(super) fn read(db: &DB) -> Result<Option<Vec<u8>>, Error> {
    Ok(db.get("store_format")?.map(|raw| {
        families()
            .position(|identity| *identity == raw)
            .map_or(raw, |index| BASE[index % BASE.len()].to_vec())
    }))
}

pub(super) fn enable_all(format: &[u8]) -> Result<&'static [u8], Error> {
    BASE.iter()
        .chain(RDF.iter())
        .chain(ALL_RDF.iter())
        .position(|identity| *identity == format)
        .map(|index| ALL_RDF[index % BASE.len()])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "complete artifact RDF requires a current authority family",
            )
        })
}

pub(super) fn enable(format: &[u8]) -> Result<&'static [u8], Error> {
    BASE.iter()
        .position(|identity| *identity == format)
        .map(|index| RDF[index])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "artifact RDF requires a current authority family",
            )
        })
}

/// Feature transitions must not discard artifact RDF's older-writer exclusion.
pub(super) fn stage(db: &DB, batch: &mut WriteBatch, base: &[u8]) -> Result<(), Error> {
    let current = db
        .get("store_format")?
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "writer format missing"))?;
    let staged = if has_intake(&current) {
        let index = base_index(base, "intake provenance fence")?;
        if has_archive(&current) {
            INTAKE_ARCHIVE[index]
        } else {
            INTAKE[index]
        }
    } else if has_archive(&current) {
        enable_archive(base)?
    } else if has_log_time(&current) {
        enable_log_time(base)?
    } else if has_tracing(&current) {
        enable_tracing(base)?
    } else if has_provenance(&current) {
        enable_provenance(base)?
    } else if has_all_artifact_rdf(&current) {
        enable_all(base)?
    } else if has_artifact_rdf(&current) {
        enable(base)?
    } else {
        base
    };
    batch.put("store_format", relift(&current, staged)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_authority_transition_preserves_the_artifact_writer_fence() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        for family in [
            RDF,
            ALL_RDF,
            PROVENANCE,
            TRACING,
            LOG_TIME,
            ARCHIVE,
            INTAKE,
            INTAKE_ARCHIVE,
            FALLBACK_TRACING,
            FALLBACK_LOG_TIME,
            FALLBACK_ARCHIVE,
            FALLBACK_INTAKE,
            FALLBACK_INTAKE_ARCHIVE,
            OWNER_GRAPH_TRACING,
            OWNER_GRAPH_LOG_TIME,
            OWNER_GRAPH_ARCHIVE,
            OWNER_GRAPH_INTAKE,
            OWNER_GRAPH_INTAKE_ARCHIVE,
            SOURCE_SUMMARY_TRACING,
            SOURCE_SUMMARY_LOG_TIME,
            SOURCE_SUMMARY_ARCHIVE,
            SOURCE_SUMMARY_INTAKE,
            SOURCE_SUMMARY_INTAKE_ARCHIVE,
            STORE_COUNTERS_TRACING,
            STORE_COUNTERS_LOG_TIME,
            STORE_COUNTERS_ARCHIVE,
            STORE_COUNTERS_INTAKE,
            STORE_COUNTERS_INTAKE_ARCHIVE,
            PAYLOAD_INDEX_TRACING,
            PAYLOAD_INDEX_LOG_TIME,
            PAYLOAD_INDEX_ARCHIVE,
            PAYLOAD_INDEX_INTAKE,
            PAYLOAD_INDEX_INTAKE_ARCHIVE,
            SEALED[0],
            SEALED[1],
            SEALED[2],
            SEALED[3],
            SEALED[4],
            SOURCE_BYTES[0],
            SOURCE_BYTES[1],
            SOURCE_BYTES[2],
            SOURCE_BYTES[3],
            SOURCE_BYTES[4],
            SEALED_SOURCE_BYTES[0],
            SEALED_SOURCE_BYTES[1],
            SEALED_SOURCE_BYTES[2],
            SEALED_SOURCE_BYTES[3],
            SEALED_SOURCE_BYTES[4],
            OWNER_SUMMARY[0],
            OWNER_SUMMARY[1],
            OWNER_SUMMARY[2],
            OWNER_SUMMARY[3],
            OWNER_SUMMARY[4],
            SEALED_OWNER_SUMMARY[0],
            SEALED_OWNER_SUMMARY[1],
            SEALED_OWNER_SUMMARY[2],
            SEALED_OWNER_SUMMARY[3],
            SEALED_OWNER_SUMMARY[4],
        ] {
            for (base, format) in BASE.into_iter().zip(family) {
                db.put("store_format", format).unwrap();
                assert_eq!(read(&db).unwrap().as_deref(), Some(base));
                for (index, next) in BASE.into_iter().enumerate() {
                    let mut batch = WriteBatch::default();
                    stage(&db, &mut batch, next).unwrap();
                    db.write(batch).unwrap();
                    assert_eq!(
                        db.get("store_format").unwrap().as_deref(),
                        Some(family[index])
                    );
                    assert_eq!(read(&db).unwrap().as_deref(), Some(next));
                }
            }
        }
        for (index, base) in BASE.into_iter().enumerate() {
            for previous in [base, RDF[index], ALL_RDF[index]] {
                assert_eq!(enable_all(previous).unwrap(), ALL_RDF[index]);
                assert_eq!(enable_provenance(previous).unwrap(), PROVENANCE[index]);
            }
            assert_eq!(
                enable_provenance(PROVENANCE[index]).unwrap(),
                PROVENANCE[index]
            );
            for previous in [base, TRACING[index], LOG_TIME[index], ARCHIVE[index]] {
                assert_eq!(enable_archive(previous).unwrap(), ARCHIVE[index]);
            }
            // Binding a log time profile never lowers an archiving store's fence.
            assert_eq!(enable_log_time(ARCHIVE[index]).unwrap(), ARCHIVE[index]);
            // Intake and archiving are independent switches; neither drops the other.
            for previous in [base, LOG_TIME[index], INTAKE[index]] {
                assert_eq!(enable_intake(previous).unwrap(), INTAKE[index]);
            }
            for previous in [ARCHIVE[index], INTAKE_ARCHIVE[index]] {
                assert_eq!(enable_intake(previous).unwrap(), INTAKE_ARCHIVE[index]);
            }
            assert_eq!(
                enable_archive(INTAKE[index]).unwrap(),
                INTAKE_ARCHIVE[index]
            );
            for family in [INTAKE, INTAKE_ARCHIVE] {
                assert_eq!(enable_log_time(family[index]).unwrap(), family[index]);
                assert!(has_log_time(family[index]));
            }
        }
        // Older binaries know none of the intake identities, so they refuse an intake store.
        for identity in INTAKE.iter().chain(INTAKE_ARCHIVE.iter()) {
            assert!(
                [BASE, RDF, ALL_RDF, PROVENANCE, TRACING, LOG_TIME, ARCHIVE]
                    .iter()
                    .all(|family| !family.contains(identity))
            );
        }
        // Older binaries know only the earlier families, so they refuse every archive identity.
        for identity in ARCHIVE {
            assert!(
                [BASE, RDF, ALL_RDF, PROVENANCE, TRACING, LOG_TIME]
                    .iter()
                    .all(|family| !family.contains(&identity))
            );
        }
        // The event fallback fence lifts every tracing-or-later family and keeps its switches.
        for (index, base) in BASE.into_iter().enumerate() {
            for (lifted, fallback) in LIFTED.iter().zip(FALLBACK.iter()) {
                assert_eq!(
                    enable_event_fallback(lifted[index]).unwrap(),
                    fallback[index]
                );
                assert_eq!(
                    enable_event_fallback(fallback[index]).unwrap(),
                    fallback[index]
                );
                assert!(has_event_fallback(fallback[index]));
                assert!(!has_event_fallback(lifted[index]));
                assert!(has_tracing(fallback[index]) && has_provenance(fallback[index]));
            }
            assert!(enable_event_fallback(base).is_err());
            assert_eq!(
                enable_log_time(FALLBACK_TRACING[index]).unwrap(),
                FALLBACK_LOG_TIME[index]
            );
            assert_eq!(
                enable_archive(FALLBACK_LOG_TIME[index]).unwrap(),
                FALLBACK_ARCHIVE[index]
            );
            assert_eq!(
                enable_intake(FALLBACK_LOG_TIME[index]).unwrap(),
                FALLBACK_INTAKE[index]
            );
            assert_eq!(
                enable_intake(FALLBACK_ARCHIVE[index]).unwrap(),
                FALLBACK_INTAKE_ARCHIVE[index]
            );
            assert_eq!(
                enable_archive(FALLBACK_INTAKE[index]).unwrap(),
                FALLBACK_INTAKE_ARCHIVE[index]
            );
        }
        // Older binaries know none of the fallback identities, so they refuse a fenced store.
        for identity in FALLBACK.iter().flatten() {
            assert!(
                [
                    BASE,
                    RDF,
                    ALL_RDF,
                    PROVENANCE,
                    TRACING,
                    LOG_TIME,
                    ARCHIVE,
                    INTAKE,
                    INTAKE_ARCHIVE
                ]
                .iter()
                .all(|family| !family.contains(identity))
            );
        }
        // The owner graph fence lifts every fallback family, keeps its switches and the
        // fallback fence, and no older binary knows its identities.
        for index in 0..BASE.len() {
            for (fallback, owner) in FALLBACK.iter().zip(OWNER_GRAPH.iter()) {
                assert_eq!(enable_owner_graphs(fallback[index]).unwrap(), owner[index]);
                assert_eq!(enable_owner_graphs(owner[index]).unwrap(), owner[index]);
                assert_eq!(enable_event_fallback(owner[index]).unwrap(), owner[index]);
                assert!(has_owner_graphs(owner[index]) && has_event_fallback(owner[index]));
                assert!(!has_owner_graphs(fallback[index]));
            }
            assert!(enable_owner_graphs(TRACING[index]).is_err());
            assert_eq!(
                enable_log_time(OWNER_GRAPH_TRACING[index]).unwrap(),
                OWNER_GRAPH_LOG_TIME[index]
            );
            assert_eq!(
                enable_archive(OWNER_GRAPH_LOG_TIME[index]).unwrap(),
                OWNER_GRAPH_ARCHIVE[index]
            );
            assert_eq!(
                enable_intake(OWNER_GRAPH_ARCHIVE[index]).unwrap(),
                OWNER_GRAPH_INTAKE_ARCHIVE[index]
            );
        }
        for identity in OWNER_GRAPH.iter().flatten() {
            assert!(
                [
                    BASE, RDF, ALL_RDF, PROVENANCE, TRACING, LOG_TIME, ARCHIVE, INTAKE
                ]
                .iter()
                .chain(LIFTED.iter())
                .chain(FALLBACK.iter())
                .all(|family| !family.contains(identity))
            );
        }
        // The source summary fence lifts every owner graph family, keeps its switches and
        // both earlier fences, and no older binary knows its identities.
        for index in 0..BASE.len() {
            for (owner, summary) in OWNER_GRAPH.iter().zip(SOURCE_SUMMARY.iter()) {
                assert_eq!(enable_source_summary(owner[index]).unwrap(), summary[index]);
                assert_eq!(
                    enable_source_summary(summary[index]).unwrap(),
                    summary[index]
                );
                assert_eq!(enable_owner_graphs(summary[index]).unwrap(), summary[index]);
                assert_eq!(
                    enable_event_fallback(summary[index]).unwrap(),
                    summary[index]
                );
                assert!(has_source_summary(summary[index]) && has_owner_graphs(summary[index]));
                assert!(has_event_fallback(summary[index]) && !has_source_summary(owner[index]));
            }
            assert!(enable_source_summary(FALLBACK_TRACING[index]).is_err());
            assert_eq!(
                enable_archive(SOURCE_SUMMARY_LOG_TIME[index]).unwrap(),
                SOURCE_SUMMARY_ARCHIVE[index]
            );
            assert_eq!(
                enable_intake(SOURCE_SUMMARY_ARCHIVE[index]).unwrap(),
                SOURCE_SUMMARY_INTAKE_ARCHIVE[index]
            );
        }
        for identity in SOURCE_SUMMARY.iter().flatten() {
            assert!(
                [
                    BASE, RDF, ALL_RDF, PROVENANCE, TRACING, LOG_TIME, ARCHIVE, INTAKE
                ]
                .iter()
                .chain(LIFTED.iter())
                .chain(FALLBACK.iter())
                .chain(OWNER_GRAPH.iter())
                .all(|family| !family.contains(identity))
            );
        }
        // The store counter fence lifts every source summary family, keeps its switches and
        // every earlier fence, and no older binary knows its identities.
        for index in 0..BASE.len() {
            for (summary, counters) in SOURCE_SUMMARY.iter().zip(STORE_COUNTERS.iter()) {
                assert_eq!(
                    enable_store_counters(summary[index]).unwrap(),
                    counters[index]
                );
                for enable in [
                    enable_store_counters,
                    enable_source_summary,
                    enable_owner_graphs,
                    enable_event_fallback,
                ] {
                    assert_eq!(enable(counters[index]).unwrap(), counters[index]);
                }
                assert!(has_store_counters(counters[index]) && has_source_summary(counters[index]));
                assert!(has_owner_graphs(counters[index]) && !has_store_counters(summary[index]));
            }
            assert!(enable_store_counters(OWNER_GRAPH_TRACING[index]).is_err());
            assert_eq!(
                enable_archive(STORE_COUNTERS_LOG_TIME[index]).unwrap(),
                STORE_COUNTERS_ARCHIVE[index]
            );
            assert_eq!(
                enable_intake(STORE_COUNTERS_ARCHIVE[index]).unwrap(),
                STORE_COUNTERS_INTAKE_ARCHIVE[index]
            );
        }
        for identity in STORE_COUNTERS.iter().flatten() {
            assert!(
                [
                    BASE, RDF, ALL_RDF, PROVENANCE, TRACING, LOG_TIME, ARCHIVE, INTAKE
                ]
                .iter()
                .chain(LIFTED.iter())
                .chain(FALLBACK.iter())
                .chain(OWNER_GRAPH.iter())
                .chain(SOURCE_SUMMARY.iter())
                .all(|family| !family.contains(identity))
            );
        }
        // The payload index fence lifts every store counter family, keeps its switches and
        // every earlier fence, and no older binary knows its identities.
        for index in 0..BASE.len() {
            for (counters, payload) in STORE_COUNTERS.iter().zip(PAYLOAD_INDEX.iter()) {
                assert_eq!(
                    enable_payload_index(counters[index]).unwrap(),
                    payload[index]
                );
                for enable in [
                    enable_payload_index,
                    enable_store_counters,
                    enable_source_summary,
                    enable_owner_graphs,
                    enable_event_fallback,
                ] {
                    assert_eq!(enable(payload[index]).unwrap(), payload[index]);
                }
                assert!(has_payload_index(payload[index]) && has_store_counters(payload[index]));
                assert!(!has_payload_index(counters[index]));
            }
            assert!(enable_payload_index(SOURCE_SUMMARY_TRACING[index]).is_err());
            assert_eq!(
                enable_archive(PAYLOAD_INDEX_LOG_TIME[index]).unwrap(),
                PAYLOAD_INDEX_ARCHIVE[index]
            );
            assert_eq!(
                enable_intake(PAYLOAD_INDEX_ARCHIVE[index]).unwrap(),
                PAYLOAD_INDEX_INTAKE_ARCHIVE[index]
            );
        }
        for identity in PAYLOAD_INDEX.iter().flatten() {
            assert!(
                [
                    BASE, RDF, ALL_RDF, PROVENANCE, TRACING, LOG_TIME, ARCHIVE, INTAKE
                ]
                .iter()
                .chain(LIFTED.iter())
                .chain(FALLBACK.iter())
                .chain(OWNER_GRAPH.iter())
                .chain(SOURCE_SUMMARY.iter())
                .chain(STORE_COUNTERS.iter())
                .all(|family| !family.contains(identity))
            );
        }
        assert!(enable(b"sparrow.store/5").is_err());
    }

    /// INVARIANT: the owner summary fence lifts exactly the source bytes families, keeps the
    /// sealed replay switch and every earlier fence in both directions, and uses identities
    /// no earlier family holds, so an older binary refuses the store.
    #[test]
    fn the_owner_summary_fence_lifts_source_bytes_and_keeps_sealed_replay() {
        for index in 0..BASE.len() {
            for family in 0..5 {
                let (plain, sealed_source) = (
                    SOURCE_BYTES[family][index],
                    SEALED_SOURCE_BYTES[family][index],
                );
                assert_eq!(
                    enable_owner_summary(plain).unwrap(),
                    OWNER_SUMMARY[family][index]
                );
                assert_eq!(
                    enable_owner_summary(sealed_source).unwrap(),
                    SEALED_OWNER_SUMMARY[family][index]
                );
                assert_eq!(
                    enable_sealed(OWNER_SUMMARY[family][index]).unwrap(),
                    SEALED_OWNER_SUMMARY[family][index]
                );
                for lifted in [
                    OWNER_SUMMARY[family][index],
                    SEALED_OWNER_SUMMARY[family][index],
                ] {
                    assert!(has_owner_summary(lifted) && has_source_bytes(lifted));
                    assert!(has_payload_index(lifted) && has_store_counters(lifted));
                    assert_eq!(enable_owner_summary(lifted).unwrap(), lifted);
                    assert_eq!(enable_source_bytes(lifted).unwrap(), lifted);
                    assert_eq!(enable_payload_index(lifted).unwrap(), lifted);
                    assert_eq!(enable_event_fallback(lifted).unwrap(), lifted);
                }
                assert_eq!(sealed(OWNER_SUMMARY[family][index]), None);
                assert!(sealed(SEALED_OWNER_SUMMARY[family][index]).is_some());
                assert!(!has_owner_summary(plain) && !has_owner_summary(sealed_source));
                assert_eq!(before_owner_summary(OWNER_SUMMARY[family][index]), plain);
                assert_eq!(
                    before_owner_summary(SEALED_OWNER_SUMMARY[family][index]),
                    sealed_source
                );
            }
            assert!(enable_owner_summary(PAYLOAD_INDEX_TRACING[index]).is_err());
            assert_eq!(
                enable_archive(OWNER_SUMMARY[1][index]).unwrap(),
                OWNER_SUMMARY[2][index]
            );
        }
        // Older binaries know none of the owner summary identities.
        let earlier = [
            BASE,
            RDF,
            ALL_RDF,
            PROVENANCE,
            TRACING,
            LOG_TIME,
            ARCHIVE,
            INTAKE,
            INTAKE_ARCHIVE,
        ];
        for identity in OWNER_SUMMARY
            .iter()
            .chain(SEALED_OWNER_SUMMARY.iter())
            .flatten()
        {
            assert!(
                earlier.iter().all(|family| !family.contains(identity))
                    && [
                        &FALLBACK,
                        &OWNER_GRAPH,
                        &SOURCE_SUMMARY,
                        &STORE_COUNTERS,
                        &PAYLOAD_INDEX,
                        &SEALED,
                        &SOURCE_BYTES,
                        &SEALED_SOURCE_BYTES,
                    ]
                    .iter()
                    .all(|families| families.iter().all(|family| !family.contains(identity)))
            );
        }
    }
}

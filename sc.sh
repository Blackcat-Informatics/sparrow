#!/bin/bash
set -e
f=store_counters.rs
sed -i 's|^use super::{Error, rdf_index, read_field, source_context, source_summary};|use super::{Error, payload_index, rdf_index, read_field, source_context, source_summary};|' $f
sed -i 's|^const SEEDED: \[&\[u8\]; 6\] = \[RECORD, ARTIFACT, GRAPH, RECORD_GRAPH, PAYLOAD, PROJECTION\];|const PAYLOAD_INDEX: \&[u8] = payload_index::PREFIX;\nconst SEEDED: [\&[u8]; 7] = [\n    RECORD,\n    ARTIFACT,\n    GRAPH,\n    RECORD_GRAPH,\n    PAYLOAD,\n    PROJECTION,\n    PAYLOAD_INDEX,\n];|' $f
sed -i 's|^    SourceContext,\n}||' $f

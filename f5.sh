#!/bin/bash
set -e
perl -0pi -e 's/    \/\/ Run the source timeline rebuild one page so it is part way.\n    store.migrate_step\(\).unwrap\(\).unwrap\(\);/    \/\/ Run the source timeline rebuild one page so it is part way, past the payload index.\n    while store.migrate_step().unwrap().unwrap().migration != Migration::SourceContext {}/' storage.rs

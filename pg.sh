#!/bin/bash
set -e
f=payload_index.rs
perl -0pi -e 's/pub\(super\) fn page\(db: &DB\) -> Result<Option<\(WriteBatch, bool\)>, Error> \{\n    let Progress::After/pub(super) fn page(db: &DB) -> Result<Option<(WriteBatch, bool)>, Error> {\n    page_of(db, PAGE_ARTIFACTS)\n}\n\n\/\/\/ [`page`] over at most `limit` artifact entries.\nfn page_of(db: &DB, limit: usize) -> Result<Option<(WriteBatch, bool)>, Error> {\n    let Progress::After/' $f
perl -0pi -e 's/if examined == PAGE_ARTIFACTS \|\| bytes >= PAGE_BYTES/if examined == limit || bytes >= PAGE_BYTES/' $f

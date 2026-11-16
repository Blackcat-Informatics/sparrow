#!/bin/bash
set -e
f=store_counters.rs
perl -0pi -e 's{/// Zero one migration.s `done` row into `batch`, for a migration reset at open: the caller\n/// moves that migration.s checkpoint to its start in the same batch and commits it directly,\n/// outside \[`apply`\], so the row and the checkpoint agree.\npub\(super\) fn seed_payload_index}{/// Zero the payload index rows into `batch`, as its fence seeds them: the caller seeds the\n/// checkpoint in the same batch and commits it directly, outside [`apply`].\npub(super) fn seed_payload_index}' $f
perl -0pi -e 's{/// Zero one migration.s `done` row, as \[`reset`\] does.\npub\(super\) fn reset}{/// Zero one migration\x27s `done` row into `batch`, for a migration reset at open: the caller\n/// moves that migration\x27s checkpoint to its start in the same batch and commits it directly,\n/// outside [`apply`], so the row and the checkpoint agree.\npub(super) fn reset}' $f

#!/bin/bash
set -e
perl -0pi -e 's/(\/\/!   \(`projection-ref:`\);\n)/$1\/\/! - `payload-index` counts the `payload-id:` keys of the payload id index (#323), which\n\/\/!   [`super::payload_index`] writes in the batch of the artifact change each follows;\n/' store_counters.rs
perl -0pi -e 's/(\/\/! - one `done:<migration>` counter per data migration)/$1/' store_counters.rs

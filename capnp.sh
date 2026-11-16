#!/bin/bash
set -e
f=sparrow.capnp
perl -0pi -e 's/(    evicting \@3 :IngestPartReceipt;[^\n]*\n  \}\n\}\n)/$1\n# Schema 71 additive (#323): the artifacts that hold one payload digest, read from the payload id\n# index. `complete` is false while the `payload-index` migration runs: every id listed is held,\n# and an absent one is unknown, never absent.\nstruct PayloadLookupQuery {\n  storeId \@0 :Text; schemaVersion \@1 :UInt32; payloadId \@2 :Text;\n  cursor \@3 :OptionalText; pageSize \@4 :UInt32 = 200;\n}\nstruct PayloadLookupPage {\n  storeId \@0 :Text; payloadId \@1 :Text; artifactIds \@2 :List(Text);\n  next \@3 :OptionalText; complete \@4 :Bool;\n}\n/' $f
perl -0pi -e 's/(  ingestPartReceipt \@55 [^\n]*\n)/$1  # Schema 71 additive (#323): the held artifacts of one payload digest, read-only. A daemon\n  # without this method answers the protocol\x27s unimplemented error.\n  payloadLookup \@56 (query :PayloadLookupQuery) -> (result :Result(PayloadLookupPage));\n/' $f

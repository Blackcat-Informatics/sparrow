#!/bin/bash
set -e
f=store_counters.rs
# Follows enum
perl -0pi -e 's/(    SourceSummaryRecords,\n    SourceContext,\n\})/    SourceSummaryRecords,\n    SourceContext,\n    PayloadIndexMigration,\n}/' $f
# Counter enum
perl -0pi -e 's/    SourceContextDone,\n\}\n\nimpl Counter \{\n    pub\(super\) const ALL: \[Self; 10\] = \[/    SourceContextDone,\n    PayloadIndex,\n    PayloadIndexDone,\n}\n\nimpl Counter {\n    pub(super) const ALL: [Self; 12] = [/' $f
perl -0pi -e 's/(        Self::SourceContextDone,\n    \];)/        Self::SourceContextDone,\n        Self::PayloadIndex,\n        Self::PayloadIndexDone,\n    ];/' $f
perl -0pi -e 's/(            Self::SourceContextDone => "done:source-context",\n)/$1            Self::PayloadIndex => "payload-index",\n            Self::PayloadIndexDone => "done:payload-index",\n/' $f
perl -0pi -e 's/(            OwnerGraphIndex, Population, SourceContext, SourceSummaryArtifacts,\n            SourceSummaryRecords,\n)/            OwnerGraphIndex, PayloadIndexMigration, Population, SourceContext,\n            SourceSummaryArtifacts, SourceSummaryRecords,\n/' $f
perl -0pi -e 's/(            Self::SourceContextDone => &\[Term \{\n                prefix: RECORD,\n                filter: Any,\n                follows: SourceContext,\n            \}\],\n)/$1            Self::PayloadIndex => &[Term {\n                prefix: PAYLOAD_INDEX,\n                filter: Any,\n                follows: Population,\n            }],\n            Self::PayloadIndexDone => &[Term {\n                prefix: ARTIFACT,\n                filter: Any,\n                follows: PayloadIndexMigration,\n            }],\n/' $f
# Positions
perl -0pi -e 's/    summary: \(Bound, Bound\),\n    context: Bound,\n\}/    summary: (Bound, Bound),\n    context: Bound,\n    payload: Bound,\n}/' $f
perl -0pi -e 's/        \|\| name == source_context::CHECKPOINT.as_bytes\(\)\n/        || name == source_context::CHECKPOINT.as_bytes()\n        || name == payload_index::CHECKPOINT.as_bytes()\n/' $f
perl -0pi -e 's/        Ok\(Self \{\n            seed,\n            owner,\n            summary,\n            context,\n        \}\)/        let payload = match payload_index::bound_progress(&mut read)? {\n            None => Bound::Everything,\n            Some(after) => Bound::after(after),\n        };\n        Ok(Self {\n            seed,\n            owner,\n            summary,\n            context,\n            payload,\n        })/' $f
perl -0pi -e 's/(            Follows::SourceContext => self.context.clone\(\),\n)/$1            Follows::PayloadIndexMigration => self.payload.clone(),\n/' $f

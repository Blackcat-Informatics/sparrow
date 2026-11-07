Refs #36

The schema 69 event-fallback open-time job now examines up to 4,096 record graphs per page, rebuilds those that need facts through one RDF writer, and commits them with its checkpoint in one synchronized write. A graph that already carries an event time is skipped without decoding its terms. Other jobs that share the pager are unchanged.

Measured on a copy of the nightly under the unit's limits, the first open fell from about 76 minutes to about 34 minutes. That is still over the 10 minute bar, so the issue stays open for the online migration duty.

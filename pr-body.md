Refs #13

A SPARQL request over a selection, a pin, or a live scope with a source, time or diagnostic filter now reads only the graphs of its own records, found through a new owner-to-graph index written beside the existing graph declarations. Rows, order, deduplication and charging are unchanged, and the whole-live-store plan is unchanged.

An open-time job builds the index for existing stores, resumable by committed page, behind five new writer-format families that fence older writers without renumbering.

The review passed. The issue stays open for UAT after the bundled deploy.

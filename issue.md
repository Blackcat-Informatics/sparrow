# Issue #341: Archive segments close on graph rows as well as records, to bound memory

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

An archive segment must also close on graph rows, so one JSON-heavy segment cannot exhaust memory.

Coordination's ruling (binding): segment selection stops adding records when the segment's quad count would pass a budget sized from the measured transient, and a single record larger than the budget goes alone in its own segment. This is a limit on how much one segment carries, not on what is stored.

Measured (#336, release, unit limits CPUQuota 100%, MemoryHigh 2G, MemoryMax 4G, NAS nfs4):
- Log-shaped: about 107 KB transient per 10,000-record segment, about 280 quads per record. Peak about 1.2 GB.
- JSON-heavy (100 keys per record, about 3,570 quads per record): transient about 220 to 390 bytes per quad. At 2,500 records (about 8.9M quads) the cgroup reached 2.15 GB and was reclaimed 5,301 times. A 10,000-record segment would need roughly 8 to 14 GB, past MemoryMax, so it would be killed.

Acceptance:
- State the budget in quads and the reasoning from the measured per-quad transient; choose it so one segment stays well under MemoryHigh (2 GiB), counting the daemon's own resident memory.
- Selection closes a segment before the budget would be passed; a record larger than the budget is archived alone.
- The rule is in the segment selection path, not the writer, and is tested with a table: a log-shaped segment is unchanged; a JSON-heavy segment closes early; an oversized single record goes alone.
- Re-run the JSON-heavy shape with the rows budget in place and report the peak (cgroup memory.peak and memory.events high).
- Report whether the GTS writer can release its 0.6 to 0.7 GB between segments cheaply. If not, state that it is held and why, rather than changing the allocator.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only

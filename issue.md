# Issue #503: Resume a direct CLI ordered file after a part receipt expires

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Refs #280 and #499. #502 resumes direct CLI ordered imports while the daemon
can still answer the original native publication. Once a pending part's native
receipt has expired and the daemon has reclaimed it, the persisted publication
ticket still forces the old publish path. The CLI keeps the file but cannot
resume that part.

On an unchanged file and against the same store, re-offer a lapsed part under
its stable request ID once the daemon confirms that the former native attempt
is no longer addressable. Retain the certified prefix and the caller's file.
Keep the staged content, durable checkpoints and the same source and format.
Use the original publication identity for uncertain replies within the retry
window. Preserve the existing refusal for an unresolved final manifest
publication after its receipt expires.

Exercise the shipped CLI after a part ticket has survived beyond the daemon's
receipt window and the native attempt has been reclaimed. The retry preserves
a held earlier part, completes the lapsed span, publishes one manifest and
reconstructs the original exactly. Document what the CLI retains and how retry
behaves. This covers the lapsed-part acceptance already recorded in #280.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only

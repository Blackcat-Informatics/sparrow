# Issue #499: Import and verify ordered part sets through the Sparrow CLI

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Refs #280. Depends on the staged import ceilings and the ordered part-set path already used by consume. [Record ceiling](http://lillith:7555/fleet/sparrow/pulls/496)

`sparrow SOCKET ingest REQUEST_ID SOURCE FORMAT FILE` accepts a completed file above the daemon's single-import bound. Reuse the existing part planner, stable part requests and manifest publisher. Keep the current ordinary artifact receipt for single-file imports. For an ordered set, return the durable manifest identity and identify its ordered parts. Store the manifest in the requested source after every part receipt is durable.

Retain the source file on refusal or interruption. Reissuing the same request resumes the recorded prefix and publishes one manifest. Expose verification in `sparrow-access`. Check each manifest member against the current artifact listing and identify missing, unavailable or mismatched members by ordinal and ID. Return a nonzero status for a partial set.

Exercise the shipped CLI against a bounded scratch producer. Verify the single-file receipt, exact reconstruction above the bound, retry after interruption, the same manifest on a repeated request and an unavailable part. Document the output, retry window and verification command. Whole-file shipper handling remains on #280 and #444.

## Syslog boundary

#282 owns the separate syslog packet limit. Exercise an above-packet-limit syslog file with the shared planner if this command takes that path. Keep the explicit single-import remedy and typed early refusal on #282 when that path remains available.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only

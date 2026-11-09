# Issue #152: Box the tail owner so release clippy passes on main

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Release clippy (`-D warnings`, clippy 0.1.100 from rustc 1.101.0-nightly 21b707e3f) refuses `enum Owner { Consume(ClaimRoot), Tail(TailRoot) }` at `crates/sparrow-server/src/watches.rs:214` with `clippy::large_enum_variant`: Tail is at least 392 bytes and Consume at least 176. Main 5813c33 is red, which blocks every commit hook (#148, #151). The fix boxes the large variant, `Owner::Tail(Box<TailRoot>)`, with no `#[allow]`. No type changed today: TailRoot's struct was last changed in a45eef0 (2026-11-01) and Owner in 8a45378 (2026-10-08).


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only

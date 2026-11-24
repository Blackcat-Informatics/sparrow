# Issue #23: Make test TCP ports collision-free across concurrent test runs

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Summary

Two TCP tests in `crates/sparrow-server/tests/rpc.rs` fail intermittently with `AddrInUse` when several checkouts run the test suite at the same time. That makes the commit hook fail for reasons unrelated to the change being committed. Make the test listener ports collision-free so the hook only fails on real defects.

## Current behavior

- `configure_tcp_listener` (`crates/sparrow-server/tests/rpc.rs:454-503`) picks ports from a process-local counter that always starts at 10240 (`NEXT_PORT`, line 457). It binds a `TcpListener` on `0.0.0.0:<port>` as a reservation, writes the port into the server config, and returns the listener.
- Tests drop that reservation before the daemon starts and binds the same port, for example at line 519 (`drop(reservation)`). Between the drop and the daemon's bind, any other process can take the port.
- Every test process on the host starts from the same 10240. Two worktrees running `make check` at once (normal when several deliveries are in flight) therefore probe the same ports in the same order and collide. This was observed on 2026-11-02, when a hook run on the `deploy` branch failed two TCP tests with `AddrInUse`, and a rerun of the same tree passed.

## Work

1. Remove the window between the reservation and the daemon's bind. Two options, in order of preference:
   - Let the test daemon take an already-bound listener. Hand the reserved socket over by file descriptor through the existing daemon launch path, or through a test-only config field that is not exposed in production config. Do not add a production knob.
   - Or start `NEXT_PORT` from a per-process offset (for example derived from the process ID across the unprivileged non-ephemeral range), and retry the daemon start on a new port when the bind fails with `AddrInUse`, failing after a bounded number of attempts.
2. Keep the existing guarantee that test ports never fall in the kernel's ephemeral range (`/proc/sys/net/ipv4/ip_local_port_range`).
3. Apply the same fix to every test that uses `configure_tcp_listener` or a similar reservation pattern. Find them with `grep -rn "drop(reservation)" crates`.

## Out of scope

Production TCP listener behavior.

## Depends on

None.

## Automated acceptance

- `tcp_test_ports_do_not_collide_across_processes`: two copies of the test binary started together, each binding several test listeners, never get `AddrInUse` from each other.
- The existing TCP tests pass unchanged in behavior.
- `make check` passes.

## UAT

1. In two worktrees on the same commit, start `cargo test --locked --release -p sparrow-server --test rpc` at the same moment, five times in a row. Expect every run in both worktrees to pass, with no `AddrInUse` in either log.
2. Commit any small change in two worktrees at once through the normal hook. Expect both hooks to pass.

Done when every UAT step passes on the live service.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only

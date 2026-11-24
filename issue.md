# Issue #85: Refuse restart.sh on a unit in transition, as start.sh does

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

- Row B-57 of `docs/open-items.md` (main 03052cf).
- Deploy review, OPTIONAL O2: `~/.local/state/sparrow-deliver/reviews/deploy-review.md:76`.
- Marked by an agent: a review OPTIONAL finding that was not taken.

## Current behavior

- `require_settled` (`deploy/lib.sh:284-293`) refuses unless the unit is `active`, `inactive` or `failed`, with the message "wait until it is active or stopped".
- `deploy/start.sh:64` calls it; `deploy/restart.sh` does not. Restart proceeds to `unit_active` (`restart.sh:60`) and `drain_stop` (`:67`) whatever the transitional state. The review judged this safe today through `drain_stop` and the backup lock, but the wording a user sees differs from `start.sh`.

## Work

Call `require_settled` in `deploy/restart.sh` before `drain_stop` and any image install, matching `start.sh`.

## Done when

`grep -n require_settled deploy/restart.sh` prints a call before the `drain_stop` line. With `SYSTEMCTL` pointed at a stub that reports `ActiveState=activating` (for example through `deploy/test-instance.sh`), `deploy/restart.sh` exits nonzero with "is activating; wait until it is active or stopped" and makes no `stop` call. Today it does not print that message.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only

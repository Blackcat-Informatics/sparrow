# Plan #175
Behavior: end of switch starts only sparrow-ship.timer; backup timer stays stopped with printed instruction; abort path restores only timers active before (rule: per-timer was-active flag recorded before the stop); wording 68 to 71; docs agree; recorded stubbed check.
Dependencies: none. Exclusions: live host never touched; no changes to lib.sh or other scripts.
Acceptance: grep shows no start of backup timer except the abort was-active restore; tools/user-unit-upgrade-timers-check.sh PASS; bash -n and shellcheck -x pass.
Risk: shell script on the live switch path.
## User rulings
- Coordination ruling 2026-11-09 20:2x (in issue): fix in the script through an issue and PR, not by stopping the timer by hand.

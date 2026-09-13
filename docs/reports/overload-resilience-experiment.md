# Overload-resilience experiment: 2000 tasks, fake harness, real scheduler

Status: results filled per scenario as each run lands (see "Run log"). Driver:
`scripts/experiments/overload_2000.py`; tests: `test/test_overload_acceptance.py`,
`test/test_overload_experiment_smoke.py`. RFC: `docs/request-for-change/rfc-overload-resilience.md`
§11 maps every acceptance row to a test name.

## 1. Environment and its limits

| Item | Value |
|---|---|
| Host | macOS (Apple Silicon), Python 3.12, one experiment process at a time |
| Harness | FAKE worker over the REAL `SubagentManager` admission, `taskq` SQLite store, `AdaptiveController` + `AdaptivePolicy`, `DependencyCoordinator`, `RecoveryLadder`, in-process `SpawnGate` + `HostBudget`, real `AcpRuntime` `StartCollector` (fake stdio pipe) |
| Clock | discrete-event virtual clock injected into every component that takes one; 2000 tasks finish in seconds of wall time |
| Not exercised | real kiro-cli, real MCP backends, AF_UNIX transport. The gatewayd `SpawnGate` is driven IN PROCESS because the sandboxed research environment refuses `AF_UNIX` sockets with `PermissionError: EPERM` (3 of 103 baseline transport tests; the B agent later found sockets work on the dev box itself, so those tests are an environment matter, not a product defect) |
| Data home | a fresh temporary `KIROCREW_HOME` per scenario; the driver refuses the default `~/.kiro/crew` (RFC §11) |
| Tonight's incidents | a concurrent full-suite `pytest -n 8` by another agent plus a 2000-task run triggered a macOS kernel watchdog panic and a reboot; two gateway restarts later cancelled this agent's continuations mid-run. Hence: one Python process at a time, `-n 0`, every driver run capped by `timeout 180`, N=500 for fault scenarios and N=2000 only for `baseline` |

## 2. Method

Every scenario submits N tasks from three dashboard sessions in BLOCKS (all of s1,
then s2, then s3 -- FIFO would starve the later sessions) plus a `system` lane
(`cron:nightly`, 4 % of N), user cap 10 (cap 2 for `tree`). The fake run walks
the same seams a real run does: `admitted -> starting -> running` in the store,
cold start through the `SpawnGate` permit and a `HostBudget` charge, a provider
call, optional nested `spawn_sub_agents` children, a body of ~5 virtual seconds,
then `_claim_finalize` (the production one-shot terminal token). Waits go through
`DependencyCoordinator.report` + `admission.yield_slot` and come back through
`request_resume` -> pump -> `resume_grant`, exactly X1's run-loop path.

Fault knobs: injected start timeouts (`controller`, `controller_raw`), a gateway
crash after N/4 completions (`restart`: store closed unflushed, every run
coroutine killed, a fresh manager reconciles on boot; plus the real
`StartCollector` adopting a late `session/new`), a gatewayd outage of 20 virtual
minutes (`outage`: the gate is unreachable, each refusal is an L1 ladder event
parked on the shared `mcp_gateway:capacity` scope), a provider 429 storm on one
scope from every session (`throttle`), and S -> A -> B trees at cap 2 with
`child_reserve=1` (`tree`).

Metrics (SPEC §五): accepted / completed / failed / cancelled /
unknown_side_effect / remaining, queue wait p50 / p95 / max (virtual seconds from
accept to first start), max in-flight, effective-cap timeline (exec cap and gate
capacity per controller decision), `HostBudget` peaks, recovery attempts by ladder
layer, restarts, fairness (per-lane first-grant index and the longest run of
other-lane starts a lane sat through with pending work), duplicate dispatches
(must be 0), lost tasks (must be 0), manual interventions (must be 0).

## 3. Results

### 3.1 Summary table

Virtual seconds throughout; wall time is the driver's own. `baseline` ran at N=2000; the fault scenarios at N=500 (one process at a time under `timeout 180` after tonight's reboot). Every row's invariants held: no lost task, no duplicate dispatch, in-flight within the cap, window <= 64, procs within the budget, zero manual interventions.

| scenario | N | done | failed | unknown_se | remaining | lost | dup | wait p50 s | wait p95 s | wait max s | max in-flight | peak procs | cap min | restarts | virtual s | wall s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 2000 | 2000 | 0 | 0 | 0 | 0 | 0 | 591.89 | 1133.58 | 1191.41 | 10 | 10 | 10 | 0 | 1197.7 | 13.14 |
| controller | 500 | 495 | 5 | 0 | 0 | 0 | 0 | 366.06 | 508.75 | 523.62 | 10 | 10 | 2 | 0 | 535.0 | 2.03 |
| controller_raw | 500 | 495 | 5 | 0 | 0 | 0 | 0 | 619.67 | 1296.01 | 1372.79 | 10 | 10 | 2 | 0 | 1380.0 | 2.23 |
| restart | 500 | 490 | 0 | 10 | 0 | 0 | 0 | 147.81 | 281.67 | 295.86 | 10 | 10 | 10 | 1 | 302.6 | 2.78 |
| outage | 500 | 500 | 0 | 0 | 0 | 0 | 0 | 24.1 | 27.55 | 27.55 | 10 | 10 | 10 | 0 | 1499.9 | 2.65 |
| throttle | 500 | 500 | 0 | 0 | 0 | 0 | 0 | 107.5 | 497.12 | 511.19 | 10 | 16 | 5 | 0 | 520.0 | 2.61 |
| tree | 600 | 600 | 0 | 0 | 0 | 0 | 0 | 436.21 | 1654.07 | 1775.68 | 2 | 6 | 2 | 0 | 1780.3 | 2.43 |

### 3.2 Effective-cap timeline (`controller`)

| t (s) | exec cap | gate cap | running | window | procs | decision |
|---|---|---|---|---|---|---|
| 0.0 | 10 | 4 | 10 | 64 | 0 |  |
| 35.0 | 10 | 5 | 10 | 64 | 8 | increase: clean window earned +1 |
| 65.0 | 5 | 3 | 10 | 64 | 10 | decrease: corroborated pressure: gate_failures,start_latency |
| 95.0 | 3 | 2 | 5 | 64 | 5 | decrease: corroborated pressure: gate_failures,start_latency |
| 125.0 | 2 | 1 | 3 | 64 | 3 | decrease: corroborated pressure: gate_failures,start_latency |
| 185.0 | 3 | 1 | 3 | 64 | 2 | increase: clean window earned +1 |
| 215.0 | 3 | 2 | 3 | 64 | 3 | increase: clean window earned +1 |
| 245.0 | 4 | 2 | 4 | 64 | 3 | increase: clean window earned +1 |
| 275.0 | 5 | 3 | 5 | 64 | 4 | increase: clean window earned +1 |
| 305.0 | 6 | 3 | 6 | 64 | 5 | increase: clean window earned +1 |
| 335.0 | 7 | 3 | 7 | 64 | 6 | increase: clean window earned +1 |
| 365.0 | 8 | 3 | 8 | 64 | 7 | increase: clean window earned +1 |
| 395.0 | 9 | 3 | 9 | 64 | 8 | increase: clean window earned +1 |
| 425.0 | 10 | 3 | 10 | 64 | 9 | increase: clean window earned +1 |
| 460.0 | 10 | 4 | 10 | 64 | 10 | increase: clean window earned +1 |
| 495.0 | 10 | 5 | 10 | 45 | 10 | increase: clean window earned +1 |

Controller decisions: `{"decrease": 3, "increase": 12, "pause": 0, "probe": 0, "resume": 0}`.

### 3.3 Fairness (`baseline`)

| lane | accepted | first-grant index | max starvation (other-lane starts) |
|---|---|---|---|
| dashboard:s1 | 640 | 0 | 3 |
| dashboard:s2 | 640 | 11 | 11 |
| dashboard:s3 | 640 | 12 | 12 |
| system | 80 | 13 | 13 |

Sessions were submitted in blocks (all of s1 first); the weighted round-robin gave every lane its first grant within 13 starts and no lane waited more than 13 other-lane starts while it had pending work.

### 3.4 Recovery (`outage`, `restart`)

**outage** (gate unreachable for 20 virtual min): failed=0, done=500, waits entered=470 {"waiting_dependency": 470}, resumes=470, infra refusals=470, scope attempts peak=12, scopes=['mcp_gateway:capacity'], ladder attempts by layer={"L1_tool_call": 470}, L4 restarts counted=0, L5 notifications=0, recovery after the daemon returned=0.0 s, peak procs during the wait=10.

**restart**: `{"after_reconcile": {"done": 125, "queued": 365, "unknown_side_effect": 10}, "at_virtual_secs": 77.1, "completed_before": 125, "in_flight_at_crash": 10, "live_before": {"queued": 365, "running": 10}, "new_incarnation": "0a1d557e", "old_incarnation": "48b42eef", "queued_survived": true, "terminal_regressed": false}`; StartCollector probe: `{"adopted_flag": true, "adopted_sessions": ["late-sid"], "gate_active_after": 0, "gate_releases": 1, "outcome": "adopted", "ran": true, "request_dropped_after": true, "request_still_owned": true}`.

**throttle** (429 storm on one provider scope from 3 sessions): reports by session={"cron:nightly": 7, "dashboard:s1": 4, "dashboard:s2": 6, "dashboard:s3": 4}, scopes seen=['mcp_gateway:capacity', 'provider:bedrock'], max live scopes=2, coordinator reports=282, wakes=282, recovery after the storm=23.6 s, cap min=5, decisions={"decrease": 5, "increase": 8, "pause": 0, "probe": 0, "resume": 0}.

**tree** (S -> A -> B, cap 2, child_reserve 1): `{"children": 200, "children_done": 200, "max_depth": 2, "roots": 100, "roots_done": 100, "waiting_children_entered": 200}`, max in-flight 2.

## 4. Defects found (both FIXED by R-glue; the two tests now must pass)

Both were reproduced by strict-`xfail` tests in `test/test_overload_acceptance.py`;
the marks are removed and `test_d1_*` / `test_d2_*` pass. Regression tests per
defect: `test/test_adaptive_policy.py::TestGateFailureWindow` (D1) and
`test/test_dependency_coordinator.py::test_infra_scope_survives_more_probes_than_max_attempts`
(D2).

**Fixes.** D1: `Sample.gate_failures_in_window` is the only thing `classify`
reads for the gate-failure signal; `AdaptivePolicy._windowed_gate_failures`
derives it from the daemon's lifetime `outcomes.failure` counter as the delta
over `Thresholds.gate_failure_window_secs` (60 s; a counter that goes down is a
daemon restart and resets the baseline), the same way the policy already diffs
`successes`. D2: `taskq/dependency.py` makes `mcp_gateway:*` scopes
(`INFRA_SCOPE_PREFIXES`, `is_infra_scope`) deadline-bounded only -- the
`max_attempts` probe cap no longer applies to our own infrastructure, one probe
per backoff step for the whole scope, `dependency_wait_deadline_secs` (3600)
ends it; and `subagent_manager/run.py::_yield_for_infra_retry_impl` passes a
`retry_at` only when the SERVER stated `retry_after_secs`, never the L1
ladder's per-run delay, so the coordinator's own jittered exponential backoff
(the recovery ladder's schedule, capped at `agent.recovery_backoff_max_secs`) governs the shared scope.

**Before / after** (same driver, `--n 500`, one process, `timeout 180`):

| scenario | before: done / failed | before: wait p50 / p95 / max s | before: virtual s | before: cap | after: done / failed | after: wait p50 / p95 / max s | after: virtual s | after: cap |
|---|---|---|---|---|---|---|---|---|
| controller_raw (D1, lifetime gate counter as the daemon reports it) | 495 / 5 | 619.67 / 1296.01 / 1372.79 | 1380.0 | 10 -> 5 -> 3 -> 2, then pinned at 2 for 1255 s (0 increases) | 495 / 5 | 366.06 / 508.75 / 523.62 | 535.0 | 10 -> 5 -> 3 -> 2 at t=125, back to 3 at t=185, 4, 5 ... (decrease 3, increase 12) -- identical to the windowed `controller` row |
| outage, ladder delay fed as `retry_at` (D2 reproduction, `infra_retry_at_from_ladder=True`, N=30 in `test_d2_*`) | 10 / 20 failed at t=63 s of a 20-min outage | -- | -- | -- | 30 / 0 | -- | -- | scope survives 20 min, all waiters resume when the daemon returns |
| outage (scope backoff governs, the shipped run.py shape) | 500 / 0 | 24.1 / 27.55 / 27.55 | 1499.9 | 10 | 500 / 0 | 24.1 / 27.55 / 27.55 | 1499.9 | 10 (unchanged: probe attempts peak 12 over 20 min) |

The 5 `failed` in both `controller` rows are the injected start timeouts
themselves (40 % of starts in the 30..90 s window), not a controller defect.

### D1 -- two lifetime gate failures pin the adaptive cap

`src/kiro_crew/adaptive/signals.py` L204 (`classify`) compares
`sample.spawn_gate.failures` against the per-window threshold `gate_failures=2`,
but that field is the daemon's LIFETIME `outcomes.failure` counter
(`src/kiro_crew/mcp_gateway/admission.py` L352 `_note_outcome`, L461 `snapshot`;
read by `SpawnGateStats.from_snapshot`, `signals.py` L106; the in-process
fallback `src/kiro_crew/adaptive/controller.py` L438 is cumulative too). With the
signal permanently on, `signals.py` L214 keeps `clear_for_increase` False, so the
exec cap never earns an increase again after its decrease -- until the daemon
restarts. Observed in `controller_raw`: cap cut to 2 and held there for the rest
of the run with 0 timeouts and completion rate 1.0. Test:
`test_d1_two_lifetime_gate_failures_must_not_pin_the_cap_forever`. Fix: window
the gate outcome counters in `build_sample` (diff against a 60 s-old snapshot) or
report windowed counts from the daemon. The `controller` scenario applies that
windowing in the harness (`Faults.gate_stats_windowed`) to measure recovery.

### D2 -- a gatewayd outage of about a minute fails every waiter on the scope

`src/kiro_crew/subagent_manager/run.py` L2320-2322 (`_yield_for_infra_retry_impl`)
turns the L1 ladder's per-RUN delay (`observe_failure(L1, unit="subagent:<id>")`,
about 2 s at attempt 1) into the shared scope's `retry_at`
(`mcp_gateway:<class>`). `src/kiro_crew/taskq/dependency.py` L556-559 honours a
supplied `retry_at` exactly, and every woken probe is a DIFFERENT run at its own
L1 attempt 1, so the scope retries every ~2 s; `dependency_max_attempts=20`
(`dependency.py` L533) then fails ALL waiters after ~60 s -- inside an outage the
3600 s wait deadline was meant to survive. Observed: 20 of 20 waiters `failed` at
t = 63 s of a 20-minute outage. Test:
`test_d2_gatewayd_outage_longer_than_a_minute_must_not_fail_the_scope`. Fix: key
the L1 ladder unit by scope, or pass `retry_at=None` so the scope's own capped
exponential backoff governs. The `outage` scenario runs with `retry_at=None`
(`Faults.infra_retry_at_from_ladder=False`) to measure recovery.

### Observations (tuning, not defects)

- O1: the controller's evidence window (60 s) is twice its decrease cooldown
  (30 s), so ONE burst of start timeouts yields 2-3 consecutive decreases before
  the evidence ages out (`10 -> 5 -> 3 -> 2`, not the RFC's illustrative
  `10 -> 6 -> 4`). A decrease could discard the failure evidence it acted on,
  as it already discards successes.
- O2: a provider-429 storm parks runs WITH their residency, so fresh starts fill
  the `HostBudget` (16 procs here); the controller then cuts the exec cap on real
  host pressure (`procs` + budget-refusal `gate_failures`), never on the throttle
  itself. Correct by design; worth knowing when reading a 429 incident.

## 5. Wait kinds -- who detects, where stored, what is released, what stays charged

Copied from DK's handoff and verified against `src/kiro_crew/taskq/waits.py`
(`WaitRecord.children/dependency/input/permission`, `WaitLedger`),
`src/kiro_crew/subagent_manager/admission/waits.py` (`yield_slot`, `request_resume`,
`resume_grant`, `taskq_child_registered`, `taskq_child_terminal`,
`taskq_expire_waits`), `src/kiro_crew/taskq/dependency.py` (`report`, `tick`,
`_fail_scope`) and `src/kiro_crew/acp/runtime.py` (`StartCollector`).

| Wait | Who detects | Where stored | Quota released | Real resources still charged | Wake event | Terminal on recovery failure |
|---|---|---|---|---|---|---|
| `waiting_children` (W3) | `admission.taskq_child_registered`: a child registers or queues under `subagent:<id>` while the parent's trusted in-flight tool name ends in `spawn_sub_agents` | row `wait_json{kind=children, ids, tool_call_id, deadline_at}`, `task_events` (`transition{wait}`, `wait_updated`, `child_settled`); children carry `parent_id`/`root_id` | lane slot (`yield_slot`: `_release_slot` + `_running_count -= 1` + pump) | parent runtime (session handle or process); `residency_charged=True` is enforced by `WaitRecord.__post_init__` | `taskq_settle(child)` -> `WaitLedger.on_child_terminal` -> LAST awaited child -> store `wake` (generation+1) -> `request_resume` (front of window) -> pump `resume_grant` by capacity | `on_child_failure=fail_parent` -> parent `failed{child_failed}` and remaining children cancelled children-first; `deadline_at` -> `failed{wait_deadline}` via `taskq_expire_waits`; parent CANCELLED cascades to children |
| `waiting_dependency` (W2) | a dependency adapter's `DependencySignal` -> `DependencyCoordinator.report`; `admission.yield_slot(persist=False)` | `wait_json{kind=at_time or signal, dependency_scope}` + one `ScopeSchedule` per scope (in memory, `rebuild()` from rows on boot) | lane slot | runtime until `WaitLedger.park` moves the row to `retry_wait` (the only write that ends the charge) | `coordinator.tick()` at the scope's `retry_at`: ONE probe, then `capacity()` per `wake_spacing_secs`; a live run resumes through `wake_through` -> `request_resume`; a parked row is re-queued | attempts > `dependency_max_attempts` or wait > `dependency_wait_deadline_secs` -> `_fail_scope` -> every waiter `failed`; `auth_failed` -> `waiting_input`; permanent errors -> `failed` at once |
| `waiting_input` (W4) | the tool layer: interactive-command classifier or `STUCK_INPUT` oracle (Linux) -> `WaitRecord.input(tool_call_id)` | `wait_json{kind=input, tool_call_id}`, `cancel_semantics=cancel_call` | lane slot | the blocked process (kept; nothing is auto-answered) | user input routed to the tool call -> `request_resume` | `deadline_at` -> `failed`; `interactive_command_policy=cancel` ends the call at the no-progress budget |
| `waiting_permission` (W5) | approval broker -> `WaitRecord.permission(approval_id)` | `wait_json{kind=permission}` | lane slot | runtime | approval decision -> `request_resume` | `deadline_at` -> `failed` |
| `retry_wait` (W6) | coordinator for a non-running row; the runner adapters' `recovering(delay)` | row `retry_wait`, `next_run_at` = scope deadline (a safety net; the staged wake is the normal path) | lane slot AND the runtime (released) | none | `next_run_at` reached or coordinator wake -> `queued` -> admission | ladder caps -> `failed`; class `unknown` -> `unknown_side_effect` |
| `recovering` (W7) / late `session/new` | `AcpRuntime.create_session` timeout -> `StartCollector` owns `req_id` (`_PendingRequests.adopt`) and the gate permit | row `recovering` (`taskq_mark`); the collector holds the outstanding request | nothing new held; the gate permit stays with the collector | the outstanding request, and the late session if created | late answer + adopter -> `adopted`, run continues on that session (verified: `outcome=adopted`, `gate_releases=1`) | `torn_down` / `abandoned` (`agent.start_collect_timeout_secs`) / `runtime_dead` -> run ends `start_abandoned` (`failed`) |
| `queued` (W1) | scheduler at accept (`taskq_accept`, write-before-ack) | row `queued`; at most `task_dispatch_window` (64) dicts in memory | none held | none | admission grants a slot (WRR across lanes, FIFO within, `child_reserve` for nested starts) | store write failure -> refused at accept (`task_store_unavailable`), never an id |

## 6. Run log

Filled in order; a scenario that exceeds `timeout 180` is recorded here as a
finding and not retried larger.

- baseline: N=500 3.55 s wall / 88 MB RSS, then N=2000 13.3 s wall / 119 MB RSS -- 2000 done, 0 failed, 0 lost, 0 dup; p50/p95/max wait 592/1134/1191 s; max in-flight 10; window 64; procs peak 10.
- controller: N=500 2.2 s wall / 89 MB -- 495 done, 5 failed (the injected start timeouts, attributable), 0 lost/dup; exec cap 10 -> 5 -> 3 -> 2 (t=65/95/125 s) then +1 per 30 s window back to 10 by t=425 s; gate 4 -> 1 -> 5; decisions decrease=3 increase=12; p50/p95 wait 366/509 s.
- controller_raw (D1 reproduction, lifetime gate counter as the daemon reports it): N=500 2.4 s wall -- same 10 -> 5 -> 3 -> 2 cuts, then the cap stays at 2 for the remaining 1255 virtual s (no increase ever earned; last decision still names gate_failures); total 1380 s vs 535 s with the windowed counter, p95 wait 1296 s vs 509 s.
- restart: N=500 3.0 s wall -- crash at t=77 s with 125 done / 10 running / 365 queued; reconcile: 125 done untouched, 365 queued survived under their ids and re-dispatched, 10 in-flight rows -> unknown_side_effect (class unknown, never silently re-run); terminal never regressed; 0 lost, 0 dup; final 490 done + 10 unknown_side_effect; StartCollector: outcome=adopted, gate_releases=1, request owned through the timeout then dropped.
- outage (gatewayd unreachable t=20 s .. 1220 s; scope backoff governs, i.e. the D2 fix candidate): N=500 2.8 s wall -- 500 done, 0 failed, 0 lost/dup; 470 runs parked in waiting_dependency on ONE scope (mcp_gateway:capacity), 470 L1 refusals, scope probe attempts peaked at 12 over 20 min (jittered 2 s -> 900 s backoff), no waiter failed, no session severed; procs peak 10 during the wait (a refused cold start holds no charge); after the daemon returned the probe woke at once and the ramp re-admitted 10 per second through admission; total 1500 virtual s.
- throttle (429 storm on provider:bedrock t=10 s .. 130 s, all 4 lanes): N=500 2.7 s wall -- 500 done, 0 failed, 0 lost/dup; 21 throttle reports across 4 lanes joined ONE provider schedule (probe then ramp); 282 waits entered / 282 resumes; parked runs kept their residency so the 16-proc HostBudget filled and the controller cut the exec cap 10 -> 9 -> 8 -> 7 -> 6 -> 5 on procs+gate_failures (host pressure, never the throttle), then +1 per window back to 10 by t=360 s; the budget refusals parked on a second scope mcp_gateway:capacity; storm-to-first-resume 23.6 s.
- tree (cap 2, child_reserve 1, every 4th root spawns S -> A -> B): N=400 roots + 200 nested = 600 accepted, 2.6 s wall -- 600 done, 0 failed/lost/dup; 100 trees of depth 2, 200 waiting_children entries and 200 resumes (each parent yielded its slot while blocked in spawn_sub_agents and was re-admitted through the pump); max in-flight 2 throughout.

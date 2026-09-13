# Reports

Measured results from experiments and fault-injection runs against the real
scheduler. A report records what was run, on what machine, with which limits,
and what came out -- numbers first, then the defects the run exposed. Reports
are evidence for an RFC's acceptance section, never a contract themselves.

| Report | What it measures |
|---|---|
| [overload-resilience-experiment.md](overload-resilience-experiment.md) | 2000-task burst over the real admission, task store, adaptive controller, dependency coordinator and recovery ladder with a fake harness and a virtual clock; fault injection (start timeouts, gateway restart, gatewayd outage, provider 429 storm, nested trees). Evidence for `rfc-overload-resilience.md` §11. |

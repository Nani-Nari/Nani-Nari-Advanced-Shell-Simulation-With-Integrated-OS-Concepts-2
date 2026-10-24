# Advanced Shell Simulation — Deliverables 1 and 2

A Python-based Unix-like shell demonstrating command execution, built-in utilities, foreground and background processes, job control (Deliverable 1), and an internal process scheduler with Round-Robin and preemptive Priority scheduling (Deliverable 2).

## Requirements
- Python 3.9 or later
- Linux, macOS, or Windows Subsystem for Linux (WSL)

> This project relies on POSIX signals and is not compatible with native Windows Command Prompt or PowerShell.

## Files
| File | Contents |
|---|---|
| `myshell.py` | The shell (Deliverable 1) extended with the `proc` and `sched` built-ins |
| `scheduler.py` | Process model, Round-Robin and Priority schedulers, timers, and metrics |
| `test_scheduler.py` | Unit tests with hand-computed expected schedules and metrics |

## Run
```bash
python3 myshell.py
python3 -m unittest -v test_scheduler.py     # run the tests
```

## Deliverable 1: built-in commands
`cd`, `pwd`, `exit`, `echo`, `clear`, `ls`, `cat`, `mkdir`, `rmdir`, `rm`, `touch`, `kill`, `jobs`, `fg`, `bg`, `stop`, `help`.

## Deliverable 2: process scheduling
The shell keeps a table of simulated processes and schedules them on a simulated CPU. One tick of CPU time is backed by `time.sleep()`; its length is configurable. Lower priority numbers mean higher priority.

| Command | Purpose |
|---|---|
| `proc add NAME BURST [PRIORITY] [ARRIVAL]` | Add a simulated process (defaults: priority 5, arrival 0) |
| `proc list` / `proc rm NAME` / `proc clear` / `proc demo` | Manage the process table or load the demo workload |
| `sched rr QUANTUM [--tick S]` | Round-Robin with a configurable time slice |
| `sched priority [--tick S]` | Preemptive Priority scheduling (heap, FCFS ties) |
| `sched ... &` | Run in the background; `proc add` then injects a process that can preempt |
| `sched ... --quiet` | Hide the event log; show only the Gantt chart and metrics |
| `sched tick S` | Set seconds per simulated tick (default 0.1) |
| `sched metrics` / `sched compare [Q ...]` / `sched status` | Show last results, compare policies, check a background run |

Each run prints an event log (ARRIVE, DISPATCH, PREEMPT, COMPLETE), a Gantt chart, and per-process response, waiting, and turnaround times with averages, CPU utilization, throughput, and context switches.

### Example
```text
proc demo
sched rr 2
sched rr 4 --tick 0.05
sched priority
sched compare 1 2 4 8
proc clear
proc add BATCH 6 5
proc add EDITOR 3 3
sched priority --tick 0.5 &
proc add URGENT 2 1          # typed while it runs: preempts BATCH
```

## Report
See `docs/` for the APA 7 reports in Word format, including terminal execution figures.

## Limitations
This is an educational shell, not a full Bash replacement. It does not support pipes, redirection, or full terminal foreground-process-group control. The scheduler schedules simulated processes rather than host processes, so that results are repeatable; context switches are counted but cost no time.

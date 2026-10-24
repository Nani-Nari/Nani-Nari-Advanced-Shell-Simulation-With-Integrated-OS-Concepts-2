#!/usr/bin/env python3
"""
myshell.py - Advanced Shell Simulation, Deliverables 1 and 2

Deliverable 1: built-in commands, foreground/background process execution,
and job control (jobs, fg, bg, stop, kill).

Deliverable 2: an internal process scheduler (see scheduler.py). The shell
keeps a table of simulated processes (`proc`) and schedules them with
Round-Robin (configurable time slice) or preemptive Priority scheduling
(`sched`), reporting waiting, turnaround, and response time.

Requires a Unix-like OS (Linux, macOS, or WSL on Windows) because it uses
POSIX process groups and signals (SIGSTOP, SIGCONT, SIGTSTP).

Run:  python3 myshell.py
"""

import os
import shlex
import signal
import stat
import subprocess
import sys
import time
from datetime import datetime

from scheduler import (BackgroundRun, PriorityScheduler, RoundRobinScheduler,
                       SimProcess, compare, format_comparison, format_gantt,
                       format_metrics)

# Canned, repeatable workload for demonstrations:
# (name, burst ticks, priority [lower = higher], arrival tick)
DEMO_WORKLOAD = [
    ("P1", 5, 2, 0),
    ("P2", 3, 1, 1),
    ("P3", 8, 4, 2),
    ("P4", 2, 1, 3),
    ("P5", 6, 2, 4),
]

# Job states
RUNNING = "Running"
STOPPED = "Stopped"
DONE = "Done"


class Job:
    """A process launched by the shell and tracked for job control."""

    def __init__(self, job_id, process, command):
        self.job_id = job_id
        self.process = process          # subprocess.Popen object
        self.pid = process.pid
        self.command = command
        self.status = RUNNING
        self.exit_code = None
        self.started = time.time()

    def __str__(self):
        return f"[{self.job_id}] {self.pid:<7} {self.status:<8} {self.command}"


class Shell:
    def __init__(self):
        self.jobs = {}               # job_id -> Job
        self.fg_job = None           # job currently in the foreground
        self.running = True
        self.last_status = 0

        # Deliverable 2: simulated process table and scheduler state
        self.workload = {}           # name -> SimProcess (insertion = FCFS order)
        self.next_sim_pid = 101
        self.tick_seconds = 0.1      # real seconds per simulated tick
        self.last_result = None
        self.active_run = None       # BackgroundRun while `sched ... &` runs
        self._background_builtin = False

        self.builtins = {
            "cd": self.cmd_cd,
            "pwd": self.cmd_pwd,
            "exit": self.cmd_exit,
            "echo": self.cmd_echo,
            "clear": self.cmd_clear,
            "ls": self.cmd_ls,
            "cat": self.cmd_cat,
            "mkdir": self.cmd_mkdir,
            "rmdir": self.cmd_rmdir,
            "rm": self.cmd_rm,
            "touch": self.cmd_touch,
            "kill": self.cmd_kill,
            "jobs": self.cmd_jobs,
            "fg": self.cmd_fg,
            "bg": self.cmd_bg,
            "stop": self.cmd_stop,
            "help": self.cmd_help,
            "proc": self.cmd_proc,
            "sched": self.cmd_sched,
        }

        # The shell itself ignores Ctrl+C and Ctrl+Z; they are forwarded
        # to the foreground job instead, so the shell keeps running.
        signal.signal(signal.SIGINT, self.handle_sigint)
        signal.signal(signal.SIGTSTP, self.handle_sigtstp)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def prompt(self):
        cwd = os.getcwd()
        home = os.path.expanduser("~")
        if cwd.startswith(home):
            cwd = "~" + cwd[len(home):]
        return f"myshell:{cwd}$ "

    def run(self):
        print("Welcome to myshell. Type 'help' for a list of commands.")
        while self.running:
            self.reap_jobs()
            try:
                line = input(self.prompt())
            except EOFError:
                print()
                self.cmd_exit([])
                break
            except KeyboardInterrupt:
                print()
                continue
            self.execute_line(line)

    def execute_line(self, line):
        line = line.strip()
        if not line or line.startswith("#"):
            return

        background = False
        if line.endswith("&"):
            background = True
            line = line[:-1].strip()
            if not line:
                self.error("syntax error near '&'")
                return

        try:
            args = shlex.split(line)
        except ValueError as e:
            self.error(f"parse error: {e}")
            return

        name = args[0]
        if name in self.builtins:
            # Only the scheduler may run in the background (in a thread), so
            # that processes can be added while it is running.
            if background and name != "sched":
                self.error(f"{name}: built-in commands cannot run in the background")
                return
            self._background_builtin = background
            try:
                self.last_status = self.builtins[name](args[1:]) or 0
            except Exception as e:  # last line of defence: never crash the shell
                self.error(f"{name}: {e}")
                self.last_status = 1
            finally:
                self._background_builtin = False
        else:
            self.launch(args, line, background)

    # ------------------------------------------------------------------
    # Process creation and job control
    # ------------------------------------------------------------------
    def launch(self, args, command_text, background):
        """Create a new process for an external command (fork + exec)."""
        try:
            # Popen performs fork() + exec() internally. start_new_session
            # places the child in its own process group so job-control
            # signals reach it without affecting the shell.
            proc = subprocess.Popen(args, start_new_session=True)
        except FileNotFoundError:
            self.error(f"{args[0]}: command not found")
            self.last_status = 127
            return
        except PermissionError:
            self.error(f"{args[0]}: permission denied")
            self.last_status = 126
            return
        except OSError as e:
            self.error(f"{args[0]}: {e.strerror}")
            self.last_status = 1
            return

        # Like bash, job numbers restart from 1 when no jobs are active.
        job_id = max(self.jobs) + 1 if self.jobs else 1
        job = Job(job_id, proc, command_text)
        self.jobs[job.job_id] = job

        if background:
            print(f"[{job.job_id}] {job.pid}")
        else:
            self.wait_foreground(job)

    def wait_foreground(self, job):
        """Wait for a foreground job to finish or stop (like wait())."""
        self.fg_job = job
        while True:
            try:
                pid, status = os.waitpid(job.pid, os.WUNTRACED)
            except ChildProcessError:
                job.status = DONE
                break
            except InterruptedError:
                continue
            if os.WIFSTOPPED(status):
                job.status = STOPPED
                print(f"\n{job}")
                break
            if os.WIFEXITED(status) or os.WIFSIGNALED(status):
                job.status = DONE
                job.exit_code = (os.WEXITSTATUS(status) if os.WIFEXITED(status)
                                 else -os.WTERMSIG(status))
                job.process.returncode = job.exit_code
                self.last_status = job.exit_code
                if os.WIFSIGNALED(status) and os.WTERMSIG(status) == signal.SIGINT:
                    print()   # move the prompt off the ^C line
                break
        self.fg_job = None
        if job.status == DONE:
            del self.jobs[job.job_id]

    def reap_jobs(self):
        """Check background jobs without blocking and report changes."""
        for job in list(self.jobs.values()):
            try:
                pid, status = os.waitpid(job.pid, os.WNOHANG | os.WUNTRACED
                                         | os.WCONTINUED)
            except ChildProcessError:
                pid, status = job.pid, 0
                job.status = DONE
            if pid == 0:
                continue
            if os.WIFSTOPPED(status):
                job.status = STOPPED
            elif os.WIFCONTINUED(status):
                job.status = RUNNING
            elif os.WIFEXITED(status) or os.WIFSIGNALED(status):
                job.status = DONE
                job.exit_code = (os.WEXITSTATUS(status) if os.WIFEXITED(status)
                                 else -os.WTERMSIG(status))
                job.process.returncode = job.exit_code
            if job.status == DONE:
                print(f"{job}  (exit code {job.exit_code})")
                del self.jobs[job.job_id]

    def get_job(self, args, command):
        """Find a job from a job id argument, or the most recent job."""
        if not self.jobs:
            self.error(f"{command}: no current jobs")
            return None
        if not args:
            return self.jobs[max(self.jobs)]
        ref = args[0].lstrip("%")
        if not ref.isdigit():
            self.error(f"{command}: invalid job id '{args[0]}'")
            return None
        job = self.jobs.get(int(ref))
        if job is None:
            self.error(f"{command}: job {ref} not found")
        return job

    def handle_sigint(self, signum, frame):
        if self.fg_job:
            os.killpg(self.fg_job.pid, signal.SIGINT)
        else:
            raise KeyboardInterrupt

    def handle_sigtstp(self, signum, frame):
        # The child runs in its own session, so the kernel would discard a
        # forwarded SIGTSTP (orphaned process group). SIGSTOP cannot be
        # ignored or discarded, so it reliably suspends the job.
        if self.fg_job:
            os.killpg(self.fg_job.pid, signal.SIGSTOP)

    # ------------------------------------------------------------------
    # Built-in commands
    # ------------------------------------------------------------------
    def error(self, message):
        print(f"myshell: {message}", file=sys.stderr)

    def cmd_cd(self, args):
        target = args[0] if args else os.path.expanduser("~")
        target = os.path.expanduser(target)
        try:
            os.chdir(target)
        except FileNotFoundError:
            self.error(f"cd: {target}: no such directory")
            return 1
        except NotADirectoryError:
            self.error(f"cd: {target}: not a directory")
            return 1
        except PermissionError:
            self.error(f"cd: {target}: permission denied")
            return 1
        return 0

    def cmd_pwd(self, args):
        print(os.getcwd())
        return 0

    def cmd_exit(self, args):
        if self.jobs:
            print(f"Terminating {len(self.jobs)} remaining job(s)...")
            for job in self.jobs.values():
                try:
                    os.killpg(job.pid, signal.SIGCONT)
                    os.killpg(job.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        print("Goodbye.")
        self.running = False
        return 0

    def cmd_echo(self, args):
        print(" ".join(args))
        return 0

    def cmd_clear(self, args):
        os.system("cls" if os.name == "nt" else "clear")
        return 0

    def cmd_ls(self, args):
        show_all = "-a" in args
        long = "-l" in args
        paths = [a for a in args if not a.startswith("-")] or ["."]
        status = 0
        for path in paths:
            try:
                entries = sorted(os.listdir(path))
            except FileNotFoundError:
                self.error(f"ls: {path}: no such file or directory")
                status = 1
                continue
            except NotADirectoryError:
                print(path)
                continue
            except PermissionError:
                self.error(f"ls: {path}: permission denied")
                status = 1
                continue
            if not show_all:
                entries = [e for e in entries if not e.startswith(".")]
            for name in entries:
                full = os.path.join(path, name)
                if long:
                    info = os.lstat(full)
                    mode = stat.filemode(info.st_mode)
                    mtime = datetime.fromtimestamp(info.st_mtime).strftime("%b %d %H:%M")
                    print(f"{mode} {info.st_size:>8} {mtime} {name}")
                else:
                    suffix = "/" if os.path.isdir(full) else ""
                    print(name + suffix)
        return status

    def cmd_cat(self, args):
        if not args:
            self.error("cat: missing file operand")
            return 1
        status = 0
        for name in args:
            try:
                with open(name, "r", errors="replace") as f:
                    sys.stdout.write(f.read())
            except FileNotFoundError:
                self.error(f"cat: {name}: no such file")
                status = 1
            except IsADirectoryError:
                self.error(f"cat: {name}: is a directory")
                status = 1
            except PermissionError:
                self.error(f"cat: {name}: permission denied")
                status = 1
        sys.stdout.flush()
        return status

    def cmd_mkdir(self, args):
        if not args:
            self.error("mkdir: missing directory operand")
            return 1
        status = 0
        for name in args:
            try:
                os.mkdir(name)
            except FileExistsError:
                self.error(f"mkdir: {name}: already exists")
                status = 1
            except FileNotFoundError:
                self.error(f"mkdir: {name}: parent directory does not exist")
                status = 1
            except PermissionError:
                self.error(f"mkdir: {name}: permission denied")
                status = 1
        return status

    def cmd_rmdir(self, args):
        if not args:
            self.error("rmdir: missing directory operand")
            return 1
        status = 0
        for name in args:
            try:
                os.rmdir(name)
            except FileNotFoundError:
                self.error(f"rmdir: {name}: no such directory")
                status = 1
            except NotADirectoryError:
                self.error(f"rmdir: {name}: not a directory")
                status = 1
            except OSError:
                self.error(f"rmdir: {name}: directory not empty")
                status = 1
        return status

    def cmd_rm(self, args):
        if not args:
            self.error("rm: missing file operand")
            return 1
        status = 0
        for name in args:
            if os.path.isdir(name):
                self.error(f"rm: {name}: is a directory (use rmdir)")
                status = 1
                continue
            try:
                os.remove(name)
            except FileNotFoundError:
                self.error(f"rm: {name}: no such file")
                status = 1
            except PermissionError:
                self.error(f"rm: {name}: permission denied")
                status = 1
        return status

    def cmd_touch(self, args):
        if not args:
            self.error("touch: missing file operand")
            return 1
        status = 0
        for name in args:
            try:
                with open(name, "a"):
                    os.utime(name, None)   # update timestamp if it exists
            except IsADirectoryError:
                os.utime(name, None)
            except (FileNotFoundError, PermissionError) as e:
                self.error(f"touch: {name}: {e.strerror}")
                status = 1
        return status

    def cmd_kill(self, args):
        if not args:
            self.error("kill: usage: kill [pid | %job_id]")
            return 1
        status = 0
        for target in args:
            if target.startswith("%"):
                job = self.get_job([target], "kill")
                if job is None:
                    status = 1
                    continue
                pid = job.pid
            elif target.isdigit():
                pid = int(target)
            else:
                self.error(f"kill: {target}: invalid process id")
                status = 1
                continue
            try:
                os.kill(pid, signal.SIGTERM)
                # Wake a stopped process so it can receive SIGTERM.
                os.kill(pid, signal.SIGCONT)
                print(f"Sent SIGTERM to process {pid}")
            except ProcessLookupError:
                self.error(f"kill: ({pid}) - no such process")
                status = 1
            except PermissionError:
                self.error(f"kill: ({pid}) - operation not permitted")
                status = 1
        time.sleep(0.1)
        self.reap_jobs()
        return status

    def cmd_jobs(self, args):
        self.reap_jobs()
        if not self.jobs:
            print("No background jobs.")
            return 0
        print(f"{'JOB':<4}{'PID':<8}{'STATUS':<9}COMMAND")
        for job in sorted(self.jobs.values(), key=lambda j: j.job_id):
            print(job)
        return 0

    def cmd_fg(self, args):
        job = self.get_job(args, "fg")
        if job is None:
            return 1
        print(job.command)
        if job.status == STOPPED:
            os.killpg(job.pid, signal.SIGCONT)
        job.status = RUNNING
        self.wait_foreground(job)
        return 0

    def cmd_bg(self, args):
        job = self.get_job(args, "bg")
        if job is None:
            return 1
        if job.status == RUNNING:
            self.error(f"bg: job {job.job_id} is already running in the background")
            return 1
        os.killpg(job.pid, signal.SIGCONT)
        job.status = RUNNING
        print(f"[{job.job_id}] {job.command} &")
        return 0

    def cmd_stop(self, args):
        """Stop (suspend) a background job, same effect as Ctrl+Z."""
        job = self.get_job(args, "stop")
        if job is None:
            return 1
        if job.status == STOPPED:
            self.error(f"stop: job {job.job_id} is already stopped")
            return 1
        os.killpg(job.pid, signal.SIGSTOP)
        job.status = STOPPED
        print(job)
        return 0

    def cmd_help(self, args):
        print("""Built-in commands:
  cd [dir]        change directory           pwd            print working directory
  echo [text]     print text                 clear          clear the screen
  ls [-a] [-l]    list files                 cat [file]     display a file
  mkdir [dir]     create a directory         rmdir [dir]    remove an empty directory
  rm [file]       remove a file              touch [file]   create/update a file
  kill [pid|%id]  terminate a process        exit           quit the shell
Job control:
  command &       run in background          jobs           list jobs
  fg [id]         bring job to foreground    bg [id]        resume stopped job in background
  stop [id]       stop (suspend) a job       Ctrl+Z         stop the foreground job
Process scheduling (simulated CPU, 1 tick = configurable seconds):
  proc add NAME BURST [PRIORITY] [ARRIVAL]   add a process (lower priority number = higher priority)
  proc list | proc rm NAME | proc clear | proc demo
  sched rr QUANTUM [--tick SECONDS]          Round-Robin with the given time slice
  sched priority [--tick SECONDS]            preemptive Priority scheduling (heap)
  sched ... &                                run in the background; 'proc add' then preempts live
  sched ... --quiet                          hide the event log, show only the results
  sched tick SECONDS | sched metrics | sched compare [Q ...] | sched status
Any other command (e.g. sleep 10, python3 script.py) runs as an external process.""")
        return 0

    # ------------------------------------------------------------------
    # Deliverable 2: simulated process table (proc)
    # ------------------------------------------------------------------
    @staticmethod
    def parse_int(text, label, minimum):
        try:
            value = int(text)
        except ValueError:
            raise ValueError(f"{label} must be an integer, got '{text}'")
        if value < minimum:
            raise ValueError(f"{label} must be >= {minimum}, got {value}")
        return value

    def add_process(self, name, burst, priority=5, arrival=0):
        proc = SimProcess(name, burst, priority, arrival,
                          pid=self.next_sim_pid, seq=self.next_sim_pid)
        self.next_sim_pid += 1
        self.workload[name] = proc
        return proc

    def cmd_proc(self, args):
        usage = "usage: proc add NAME BURST [PRIORITY] [ARRIVAL] | list | rm NAME | clear | demo"
        if not args:
            self.error(f"proc: {usage}")
            return 1
        sub, rest = args[0], args[1:]

        if sub == "add":
            if not 2 <= len(rest) <= 4:
                self.error(f"proc: {usage}")
                return 1
            name = rest[0]
            if name in self.workload:
                self.error(f"proc: a process named '{name}' already exists")
                return 1
            try:
                burst = self.parse_int(rest[1], "burst time", 1)
                priority = self.parse_int(rest[2], "priority", 0) if len(rest) > 2 else 5
                arrival = self.parse_int(rest[3], "arrival time", 0) if len(rest) > 3 else 0
            except ValueError as e:
                self.error(f"proc: {e}")
                return 1
            proc = self.add_process(name, burst, priority, arrival)
            run = self.active_run
            if run is not None and run.is_alive():
                # A scheduler is running in the background: the new process
                # arrives now and may preempt the running process.
                run.scheduler.inject(proc)
                print(f"Added {name} (pid {proc.pid}, burst {burst}, priority "
                      f"{priority}) to the running scheduler")
            else:
                print(f"Added {name} (pid {proc.pid}, burst {burst}, "
                      f"priority {priority}, arrival {arrival})")
            return 0

        if sub == "list":
            if not self.workload:
                print("Process table is empty. Use 'proc add' or 'proc demo'.")
                return 0
            print(f"{'PID':<5}{'NAME':<6}{'BURST':>6}{'PRIORITY':>9}{'ARRIVAL':>8}")
            for p in self.workload.values():
                print(f"{p.pid:<5}{p.name:<6}{p.burst:>6}{p.priority:>9}{p.arrival:>8}")
            return 0

        if sub == "rm":
            if len(rest) != 1:
                self.error("proc: usage: proc rm NAME")
                return 1
            if self.workload.pop(rest[0], None) is None:
                self.error(f"proc: no process named '{rest[0]}'")
                return 1
            print(f"Removed {rest[0]}")
            return 0

        if sub == "clear":
            self.workload.clear()
            print("Process table cleared.")
            return 0

        if sub == "demo":
            self.workload.clear()
            for name, burst, priority, arrival in DEMO_WORKLOAD:
                self.add_process(name, burst, priority, arrival)
            print(f"Loaded demo workload ({len(DEMO_WORKLOAD)} processes).")
            return self.cmd_proc(["list"])

        self.error(f"proc: unknown subcommand '{sub}'. {usage}")
        return 1

    # ------------------------------------------------------------------
    # Deliverable 2: scheduling (sched)
    # ------------------------------------------------------------------
    def scheduler_busy(self):
        return self.active_run is not None and self.active_run.is_alive()

    def cmd_sched(self, args):
        usage = ("usage: sched rr QUANTUM [--tick S] | priority [--tick S] | "
                 "tick S | metrics | compare [Q ...] | status")
        if not args:
            self.error(f"sched: {usage}")
            return 1
        sub, rest = args[0], list(args[1:])

        # Optional --tick override for this run only
        tick = self.tick_seconds
        if "--tick" in rest:
            i = rest.index("--tick")
            try:
                tick = float(rest[i + 1])
                if tick < 0:
                    raise ValueError
            except (IndexError, ValueError):
                self.error("sched: --tick needs a non-negative number of seconds")
                return 1
            del rest[i:i + 2]
        # Optional --quiet: suppress the per-event log, show only the results
        log = print
        if "--quiet" in rest:
            rest.remove("--quiet")
            log = lambda _msg: None  # noqa: E731

        if sub == "tick":
            try:
                value = float(rest[0])
                if value < 0:
                    raise ValueError
            except (IndexError, ValueError):
                self.error("sched: usage: sched tick SECONDS (non-negative)")
                return 1
            self.tick_seconds = value
            print(f"One simulated tick now lasts {value:g} second(s).")
            return 0

        if sub == "status":
            if self.scheduler_busy():
                s = self.active_run.scheduler
                print(f"{s.name} running in background, clock t={s.clock}")
            else:
                print("No scheduler is running.")
            return 0

        if sub == "metrics":
            if self.last_result is None:
                self.error("sched: no completed scheduling run yet")
                return 1
            print(format_gantt(self.last_result))
            print(format_metrics(self.last_result))
            return 0

        if sub in ("rr", "priority", "compare"):
            if not self.workload:
                self.error("sched: process table is empty (use 'proc add' or 'proc demo')")
                return 1
            if self.scheduler_busy():
                self.error("sched: a scheduler is already running (see 'sched status')")
                return 1

        if sub == "compare":
            try:
                quanta = [self.parse_int(q, "time slice", 1) for q in rest] or [1, 2, 4, 8]
            except ValueError as e:
                self.error(f"sched: {e}")
                return 1
            print(format_comparison(compare(list(self.workload.values()), quanta)))
            return 0

        if sub == "rr":
            if len(rest) != 1:
                self.error("sched: usage: sched rr QUANTUM [--tick SECONDS]")
                return 1
            try:
                quantum = self.parse_int(rest[0], "time slice", 1)
            except ValueError as e:
                self.error(f"sched: {e}")
                return 1
            scheduler = RoundRobinScheduler(list(self.workload.values()), quantum,
                                            tick_seconds=tick, log=log)
        elif sub == "priority":
            if rest:
                self.error("sched: usage: sched priority [--tick SECONDS]")
                return 1
            scheduler = PriorityScheduler(list(self.workload.values()),
                                          tick_seconds=tick, log=log)
        else:
            self.error(f"sched: unknown subcommand '{sub}'. {usage}")
            return 1

        if self._background_builtin:
            print(f"[sched] {scheduler.name} running in background; "
                  f"'proc add' injects new processes", flush=True)
            self.active_run = BackgroundRun(scheduler, self.scheduler_finished)
            self.active_run.start()
            time.sleep(0.05)     # let the first events print before the prompt
            return 0

        try:
            result = scheduler.run()
        except KeyboardInterrupt:
            print("\nmyshell: scheduling interrupted")
            return 130
        self.last_result = result
        print(format_gantt(result))
        print(format_metrics(result))
        return 0

    def scheduler_finished(self, run):
        if run.error is not None:
            self.error(f"sched: {run.error}")
            return
        self.last_result = run.result
        print(format_gantt(run.result))
        print(format_metrics(run.result))
        print("[sched] background scheduling finished ('sched metrics' to view again)")
        # Re-display the prompt, like a shell's asynchronous job notification.
        sys.stdout.write(self.prompt())
        sys.stdout.flush()


if __name__ == "__main__":
    if os.name == "nt":
        print("myshell requires Linux, macOS, or WSL (POSIX signals).")
        sys.exit(1)
    Shell().run()

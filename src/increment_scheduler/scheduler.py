from __future__ import annotations

import argparse
from datetime import datetime, time as dt_time, timedelta
import multiprocessing as mp
import signal
import time
from typing import Callable
from zoneinfo import ZoneInfo


class ShutdownRequested(Exception):
    """Raised when a termination signal is received."""


def parse_hhmm(value: str) -> dt_time:
    parts = value.strip().split(":")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(f"Expected HH:MM time, got '{value}'")
    try:
        hour, minute = int(parts[0]), int(parts[1])
        return dt_time(hour=hour, minute=minute)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid HH:MM time '{value}'") from exc


def resolve_window(
    start_hhmm: dt_time,
    stop_hhmm: dt_time,
    tz: ZoneInfo,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    """
    Resolve start and stop datetimes in `tz`.
    If `stop_hhmm` <= `start_hhmm`, the window spans midnight to the next day.
    If `now` is already past `stop_hhmm` today and the window doesn't span midnight,
    shift both to tomorrow so a manually launched script doesn't immediately exit.
    """
    current = now.astimezone(tz) if now else datetime.now(tz)
    base_date = current.date()

    start_dt = datetime.combine(base_date, start_hhmm, tzinfo=tz)
    spans_midnight = stop_hhmm <= start_hhmm
    stop_date = base_date + timedelta(days=1) if spans_midnight else base_date
    stop_dt = datetime.combine(stop_date, stop_hhmm, tzinfo=tz)

    if not spans_midnight and current > stop_dt:
        start_dt += timedelta(days=1)
        stop_dt += timedelta(days=1)

    return start_dt, stop_dt


def next_minute_boundary(after: datetime) -> datetime:
    """Return the exact next :00-second minute boundary strictly after `after`."""
    truncated = after.replace(second=0, microsecond=0)
    return truncated + timedelta(minutes=1)


def sleep_until(target: datetime, tz: ZoneInfo, stop_flag: list[bool], poll_step: float = 0.2) -> None:
    """Sleep until `target`, waking periodically so signals are handled promptly."""
    while not stop_flag[0]:
        now = datetime.now(tz)
        remaining = (target - now).total_seconds()
        if remaining <= 0:
            return
        time.sleep(min(remaining, poll_step))


def _child_entry(task_fn: Callable[[datetime], None], scheduled_for: datetime) -> None:
    """Reset signal handlers in child so parent controls lifecycle."""
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    task_fn(scheduled_for)


def run_task_with_timeout(
    ctx: mp.context.BaseContext,
    task_fn: Callable[[datetime], None],
    scheduled_for: datetime,
    max_task_seconds: float,
) -> tuple[str, float]:
    """
    Run `task_fn(scheduled_for)` in a separate process and kill it if it exceeds
    `max_task_seconds`. Returns a status string ('ok', 'timeout', 'error') and elapsed time.
    """
    start_mono = time.monotonic()
    proc = ctx.Process(target=_child_entry, args=(task_fn, scheduled_for))
    proc.start()
    proc.join(timeout=max_task_seconds)

    if proc.is_alive():
        proc.kill()
        proc.join()
        return "timeout", time.monotonic() - start_mono

    elapsed = time.monotonic() - start_mono
    if proc.exitcode == 0:
        return "ok", elapsed
    return f"exit_{proc.exitcode}", elapsed


def sample_task(scheduled_for: datetime) -> None:
    """Replace this with your actual per-minute work."""
    print(f"[worker] executing task scheduled for {scheduled_for.isoformat()}")
    # Simulate variable duration (e.g., sub-second to a few seconds)
    print(f"EXECUTING TASK!!!")
    time.sleep(0.25)


def run_scheduler(
    start_hhmm: dt_time,
    stop_hhmm: dt_time,
    tz_name: str,
    max_task_seconds: float,
    task_fn: Callable[[datetime], None] = sample_task,
) -> None:
    tz = ZoneInfo(tz_name)
    stop_flag = [False]

    def _on_signal(signum: int, _frame: object) -> None:
        print(f"\n[scheduler] received signal {signum}, shutting down...")
        stop_flag[0] = True

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    start_dt, stop_dt = resolve_window(start_hhmm, stop_hhmm, tz)
    now = datetime.now(tz)

    if now >= stop_dt:
        print(f"[scheduler] current time ({now.isoformat()}) is already at/past stop time ({stop_dt.isoformat()}). Exiting.")
        return

    if now < start_dt:
        print(f"[scheduler] idling until start time: {start_dt.isoformat()}")
        sleep_until(start_dt, tz, stop_flag)
        if stop_flag[0]:
            return

    # Align first tick to the next minute boundary at or after start_dt
    current = datetime.now(tz)
    if current.second == 0 and current.microsecond == 0 and current >= start_dt:
        next_tick = current
    else:
        next_tick = next_minute_boundary(max(current, start_dt))

    # Use 'spawn' across OSes for predictable behavior with threads/locks/DBs
    ctx = mp.get_context("spawn")
    active_proc: mp.Process | None = None

    print(f"[scheduler] active window: {start_dt.isoformat()} -> {stop_dt.isoformat()} (max task: {max_task_seconds}s)")

    try:
        while not stop_flag[0]:
            now = datetime.now(tz)
            if now >= stop_dt:
                print(f"[scheduler] reached stop time ({stop_dt.isoformat()}). Exiting.")
                break

            if next_tick > stop_dt:
                sleep_until(stop_dt, tz, stop_flag)
                break

            if now < next_tick:
                sleep_until(next_tick, tz, stop_flag)
                continue

            tick_time = next_tick
            next_tick = tick_time + timedelta(minutes=1)

            start_mono = time.monotonic()
            proc = ctx.Process(target=_child_entry, args=(task_fn, tick_time))
            active_proc = proc
            proc.start()

            # Poll in short slices so SIGINT/SIGTERM can kill a running child immediately
            killed = False
            while proc.is_alive():
                if stop_flag[0]:
                    proc.kill()
                    proc.join()
                    killed = True
                    break
                if (time.monotonic() - start_mono) >= max_task_seconds:
                    proc.kill()
                    proc.join()
                    killed = True
                    print(f"[scheduler] KILLED task for {tick_time.strftime('%H:%M:%S')} (exceeded {max_task_seconds}s)")
                    break
                time.sleep(0.05)

            active_proc = None
            elapsed = time.monotonic() - start_mono

            if not killed and not stop_flag[0]:
                status = "ok" if proc.exitcode == 0 else f"failed(code={proc.exitcode})"
                print(f"[scheduler] tick {tick_time.strftime('%H:%M:%S')} -> {status} in {elapsed:.3f}s")

    finally:
        if active_proc is not None and active_proc.is_alive():
            active_proc.kill()
            active_proc.join()
        print("[scheduler] stopped cleanly.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a task on every minute boundary within a time window.")
    parser.add_argument("--start", required=True, type=parse_hhmm, help="Start time in HH:MM (24h), e.g. 09:30")
    parser.add_argument("--stop", required=True, type=parse_hhmm, help="Stop time in HH:MM (24h), e.g. 16:00")
    parser.add_argument("--tz", default="America/New_York", help="IANA timezone name (default: America/New_York)")
    parser.add_argument(
        "--max-task-seconds",
        type=float,
        default=15.0,
        help="Kill task if it runs longer than this many seconds (default: 15.0)",
    )
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    run_scheduler(
        start_hhmm=args.start,
        stop_hhmm=args.stop,
        tz_name=args.tz,
        max_task_seconds=args.max_task_seconds,
    )
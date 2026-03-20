import os
import signal
import time
import traceback
from datetime import datetime, timezone

from run_pipeline import main


shutdown_requested = False


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def handle_shutdown(signum, _frame) -> None:
    global shutdown_requested
    shutdown_requested = True
    print(f"[{timestamp()}] Received signal {signum}; shutting down after the current cycle.")


def sleep_with_shutdown(total_seconds: int) -> None:
    remaining = max(total_seconds, 0)
    while remaining > 0 and not shutdown_requested:
        time.sleep(min(remaining, 1))
        remaining -= 1


def run_forever() -> None:
    interval_seconds = max(int(os.getenv("RUN_INTERVAL_SECONDS", "1800")), 60)

    while not shutdown_requested:
        print(f"[{timestamp()}] Starting pipeline run.")
        try:
            main()
        except Exception as exc:
            print(f"[{timestamp()}] Pipeline run failed: {exc}")
            traceback.print_exc()

        if shutdown_requested:
            break

        print(f"[{timestamp()}] Sleeping for {interval_seconds} seconds before the next run.")
        sleep_with_shutdown(interval_seconds)

    print(f"[{timestamp()}] Worker stopped.")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    run_forever()

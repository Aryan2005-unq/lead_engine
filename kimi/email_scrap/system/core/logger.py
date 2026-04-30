"""
Structured Logger for Pipeline Workers
Outputs: [WORKER_NAME] [STATUS] [TIME] message
"""
import logging
import time
from datetime import datetime


class WorkerLogger:
    """Structured logger with performance tracking for pipeline workers."""

    def __init__(self, worker_name: str):
        self.worker_name = worker_name
        self._logger = logging.getLogger(f"pipeline.{worker_name}")
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter(
                f"%(asctime)s [{worker_name.upper()}] [%(levelname)s] %(message)s",
                datefmt="%H:%M:%S",
            )
            handler.setFormatter(fmt)
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.INFO)

        # performance counters
        self.total_processed = 0
        self.total_success = 0
        self.total_failed = 0
        self._batch_start: float = 0

    # ── Convenience methods ──
    def info(self, msg: str):
        self._logger.info(msg)

    def warn(self, msg: str):
        self._logger.warning(msg)

    def error(self, msg: str):
        self._logger.error(msg)

    # ── Batch timing ──
    def batch_start(self, size: int):
        self._batch_start = time.time()
        self.info(f"Batch started  | size={size}")

    def batch_end(self, success: int, failed: int):
        elapsed = time.time() - self._batch_start
        self.total_processed += success + failed
        self.total_success += success
        self.total_failed += failed
        rate = success / elapsed if elapsed > 0 else 0
        self.info(
            f"Batch finished | ok={success} fail={failed} "
            f"time={elapsed:.2f}s rate={rate:.1f}/s "
            f"| lifetime ok={self.total_success} fail={self.total_failed}"
        )

    def idle(self):
        self.info("Queue empty — sleeping")

    def shutdown(self):
        self.info(
            f"Shutting down   | lifetime processed={self.total_processed} "
            f"ok={self.total_success} fail={self.total_failed}"
        )

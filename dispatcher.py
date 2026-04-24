"""
Dispatcher — the main loop that ties polling, execution, and concurrency together.

The dispatcher runs an infinite loop:
    1. Poll SQS for messages
    2. For each message, spawn a task (limited by semaphore)
    3. Each task: execute code → save report → delete SQS message

The asyncio.Semaphore ensures at most N sandboxes run concurrently.
"""
import asyncio
import logging
import signal
import time

import redis as redis_client

from config import WorkerConfig
from executor import Executor
from poller import SQSPoller
from models import ExecutionRequest, ExecutionReport, MalformedMessage, StatusCode

logger = logging.getLogger(__name__)


class Dispatcher:
    """
    Coordinates polling and concurrent execution.

    The semaphore is the key mechanism:
    - Semaphore(4) means at most 4 tasks can be "inside" the semaphore
    - When a 5th task tries to acquire the semaphore, it waits until
      one of the 4 finishes and releases it
    - This prevents overloading the machine with too many sandboxes
    """

    def __init__(self, config: WorkerConfig):
        self.config = config
        self.executor = Executor(config)
        self.poller = SQSPoller(config.sqs)
        self.semaphore = asyncio.Semaphore(config.execution.max_concurrent)
        self._running = True
        self._active_tasks: set = set()
        self.redis = redis_client.Redis(
            host=config.redis.host,
            port=config.redis.port,
            decode_responses=True,
        )

    async def start(self) -> None:
        """
        Start the dispatch loop.

        This runs forever until stopped by a signal (SIGTERM/SIGINT)
        or an unrecoverable error.
        """
        logger.info(
            "Dispatcher starting (max_concurrent=%d)",
            self.config.execution.max_concurrent,
        )

        # Register signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_shutdown)

        consecutive_errors = 0

        while self._running:
            try:
                # ---- Poll for messages ----
                # This runs in a thread because boto3 is synchronous.
                # run_in_executor() moves the blocking call to a thread pool
                # so the event loop stays responsive.
                requests, malformed = await asyncio.get_running_loop().run_in_executor(
                    None,  # Use default thread pool
                    self.poller.poll,
                )

                consecutive_errors = 0  # Reset on successful poll

                # ---- Handle malformed messages ----
                for bad in malformed:
                    self._handle_malformed(bad)

                if not requests:
                    # No valid messages — loop back to poll again
                    continue

                # ---- Spawn a task for each message ----
                for request in requests:
                    task = asyncio.create_task(
                        self._handle_request(request)
                    )
                    self._active_tasks.add(task)
                    # Auto-cleanup: remove the task from the set when it finishes
                    task.add_done_callback(self._active_tasks.discard)

            except Exception as e:
                consecutive_errors += 1
                logger.error(
                    "Polling error (%d/%d): %s",
                    consecutive_errors, self.config.max_consecutive_errors, e,
                )

                if consecutive_errors >= self.config.max_consecutive_errors:
                    logger.critical(
                        "Too many consecutive errors (%d), shutting down",
                        consecutive_errors,
                    )
                    break

                # Exponential backoff: 2s, 4s, 8s, 16s, 32s
                wait_time = min(2 ** consecutive_errors, 32)
                logger.info("Backing off for %ds", wait_time)
                await asyncio.sleep(wait_time)

        # Wait for any in-flight tasks to finish
        await self._drain()

    async def _handle_request(self, request: ExecutionRequest) -> None:
        """
        Process a single execution request.

        The semaphore controls concurrency:
        - `async with self.semaphore` blocks if N sandboxes are already running
        - Once a slot opens up, it proceeds with execution
        - The slot is released when the `async with` block exits (even on error)
        """
        async with self.semaphore:
            job_id = request.job_id
            logger.info("[%s] Acquired execution slot", job_id)

            try:
                report = await self.executor.execute(request)

                # Publish to Redis — retries internally, raises if all attempts fail
                await asyncio.get_running_loop().run_in_executor(
                    None,
                    self.publish_to_redis,
                    report,
                )

                # Only delete from SQS after successful publish — if publish
                # failed, the exception above prevents reaching this line and
                # SQS will redeliver the message after visibility_timeout
                await asyncio.get_running_loop().run_in_executor(
                    None,
                    self.poller.delete,
                    request.receipt_handle,
                    request.job_id,
                )

            except Exception as e:
                # Covers: execution errors, Redis publish failure, SQS delete failure.
                # Not deleting means SQS retries the job after visibility_timeout.
                logger.error("[%s] Failed: %s", job_id, e)

    def _handle_shutdown(self) -> None:
        """
        Called when SIGTERM or SIGINT is received.

        Sets the running flag to False so the main loop exits after
        the current poll completes. In-flight tasks finish gracefully.
        """
        logger.info("Shutdown signal received, stopping after current tasks...")
        self._running = False

    async def _drain(self) -> None:
        """Wait for all in-flight tasks to complete before exiting."""
        if self._active_tasks:
            logger.info("Waiting for %d active task(s) to finish...", len(self._active_tasks))
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
            logger.info("All tasks finished")

    def _handle_malformed(self, bad: MalformedMessage) -> None:
        """
        Mark a malformed SQS message as FAILED in Redis (if job_id is known),
        then delete it from SQS so it doesn't block the queue.
        """
        if bad.job_id and self.config.redis.host:
            key = f"{self.config.redis.job_key_prefix}:{bad.job_id}"
            try:
                self.redis.hset(key, mapping={"status": "FAILED", "workerType": "NSJAIL_WORKER"})
                logger.info("[%s] Malformed message — marked job as FAILED", bad.job_id)
            except Exception as e:
                logger.error("[%s] Failed to mark malformed job as FAILED in Redis: %s", bad.job_id, e)
        else:
            logger.warning(
                "Malformed SQS message with no extractable job_id — cannot update Redis (reason: %s)",
                bad.reason,
            )

        self.poller.delete(bad.receipt_handle, bad.job_id or "")

    def publish_to_redis(self, report: ExecutionReport) -> None:
        """
        Update the job's Redis hash with the execution outcome.

        The backend creates this hash when the request is received and sets
        status=QUEUED. The worker updates it to either COMPLETED or FAILED.

        COMPLETED: execution finished (any valid outcome — accepted, wrong answer,
                   TLE, compile error, etc.). Includes the full execution report.
        FAILED:    worker-level failure (nsjail broken, disk full, etc.).
                   No report added — the job cannot be retried meaningfully.

        Raises RuntimeError after 3 failed attempts so the dispatcher skips
        SQS deletion and the message is redelivered, keeping status=QUEUED.
        """
        if not self.config.redis.host:
            logger.debug("[%s] Redis not configured — skipping publish", report.execution_id)
            return

        key = f"{self.config.redis.job_key_prefix}:{report.execution_id}"

        is_worker_failure = report.status_code in (
            StatusCode.INTERNAL_ERROR,
            StatusCode.UNSUPPORTED_LANGUAGE,
        )

        if is_worker_failure:
            mapping = {"status": "FAILED", "workerType": "NSJAIL_WORKER"}
            logger.info("[%s] Marking job as FAILED (status_code=%s)", report.execution_id, report.status_code)
        else:
            mapping = {
                "status": "COMPLETED",
                "workerType": "NSJAIL_WORKER",
                "executionReport": report.to_json(),
            }
            logger.info("[%s] Marking job as COMPLETED (status_code=%s)", report.execution_id, report.status_code)

        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                self.redis.hset(key, mapping=mapping)
                self.redis.expire(key, self.config.redis.report_ttl)
                return
            except Exception as e:
                logger.warning("[%s] Redis publish attempt %d/%d failed: %s", report.execution_id, attempt, max_attempts, e)
                if attempt < max_attempts:
                    time.sleep(attempt)  # 1s, 2s

        raise RuntimeError(f"[{report.execution_id}] Redis publish failed after {max_attempts} attempts — job stays QUEUED")

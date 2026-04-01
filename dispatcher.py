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

from config import WorkerConfig
from executor import Executor
from poller import SQSPoller
from models import ExecutionRequest

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
                requests = await asyncio.get_running_loop().run_in_executor(
                    None,  # Use default thread pool
                    self.poller.poll,
                )

                consecutive_errors = 0  # Reset on successful poll

                if not requests:
                    # No messages — loop back to poll again
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
                await self.executor.execute(request)

                # Execution succeeded — delete the message from SQS
                # so it's not retried
                await asyncio.get_running_loop().run_in_executor(
                    None,
                    self.poller.delete,
                    request.receipt_handle,
                    request.job_id,
                )

            except Exception as e:
                # If execution fails, we do NOT delete the message.
                # SQS will make it visible again after visibility_timeout
                # and another worker (or this one) will retry.
                logger.error("[%s] Execution failed: %s", job_id, e)

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

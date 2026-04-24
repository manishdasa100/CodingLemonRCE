"""
SQS Poller — receives code execution requests from the queue.

Uses long polling: the API call waits up to `poll_wait_time` seconds
for messages to arrive before returning empty. This avoids hammering
SQS in a tight loop when the queue is empty, while still responding
quickly when messages arrive.
"""
import json
import logging
from typing import List, Tuple

import boto3

from config import SQSConfig
from models import ExecutionRequest, MalformedMessage, parse_sqs_message

logger = logging.getLogger(__name__)


class SQSPoller:
    """
    Polls SQS for code execution requests.

    Usage:
        poller = SQSPoller(sqs_config)
        requests = poller.poll()                      # Returns list of ExecutionRequest
        poller.delete(request.receipt_handle)         # Remove from queue after processing
    """

    def __init__(self, config: SQSConfig):
        self.config = config
        self.client = boto3.client(
            "sqs",
            region_name=config.region,
        )

    def poll(self) -> Tuple[List[ExecutionRequest], List[MalformedMessage]]:
        """
        Receive up to max_messages_per_poll messages from SQS.

        Returns a tuple of (valid_requests, malformed_messages).

        Malformed messages are returned (not silently deleted) so the
        dispatcher can mark the job as FAILED in Redis before deleting.
        If a job_id can be extracted from the partial JSON it is included
        so Redis can be updated; otherwise job_id is None.
        """
        try:
            response = self.client.receive_message(
                QueueUrl=self.config.queue_url,
                MaxNumberOfMessages=self.config.max_messages_per_poll,
                WaitTimeSeconds=self.config.poll_wait_time,
                VisibilityTimeout=self.config.visibility_timeout,
            )

            messages = response.get("Messages", [])
            if not messages:
                return [], []

            logger.info("Received %d message(s) from SQS", len(messages))

            requests: List[ExecutionRequest] = []
            malformed: List[MalformedMessage] = []

            for msg in messages:
                receipt_handle = msg["ReceiptHandle"]
                try:
                    request = parse_sqs_message(
                        body=msg["Body"],
                        receipt_handle=receipt_handle,
                    )
                    requests.append(request)
                except (KeyError, ValueError) as e:
                    job_id = self._try_extract_job_id(msg.get("Body", ""))
                    logger.error(
                        "Malformed SQS message (MessageId=%s, job_id=%s): %s",
                        msg.get("MessageId", "unknown"), job_id or "unknown", e,
                    )
                    malformed.append(MalformedMessage(
                        receipt_handle=receipt_handle,
                        job_id=job_id,
                        reason=str(e),
                    ))

            return requests, malformed

        except Exception as e:
            logger.error("SQS polling failed: %s", e)
            raise

    def _try_extract_job_id(self, body: str) -> str | None:
        """Best-effort extraction of jobId from a potentially malformed message body."""
        try:
            return json.loads(body).get("jobId")
        except Exception:
            return None

    def delete(self, receipt_handle: str, job_id: str = "") -> None:
        """
        Delete a message from SQS after successful processing.

        Once we delete a message, SQS will never deliver it again.
        If we DON'T delete it (e.g., we crash mid-execution), the
        message becomes visible again after visibility_timeout expires
        and another worker can pick it up. This is SQS's built-in
        retry mechanism.
        """
        if not receipt_handle:
            logger.warning("No receipt handle for job_id=%s — cannot delete from SQS", job_id or "unknown")
            return

        try:
            self.client.delete_message(
                QueueUrl=self.config.queue_url,
                ReceiptHandle=receipt_handle,
            )
            logger.debug("Deleted message from SQS with (job_id=%s), (receipt_handle=%s)", job_id or "unknown", receipt_handle)
        except Exception as e:
            logger.error("Failed to delete SQS message with (job_id=%s) (receipt_handle=%s): %s", job_id or "unknown", receipt_handle, e)

import json
import base64
import time
from enum import IntEnum
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any


# ---------------------------------------------------------------------------
# Status codes — these match what your Java/Kotlin backend expects
# ---------------------------------------------------------------------------

class StatusCode(IntEnum):
    """Numeric codes your backend uses to classify the outcome."""
    ACCEPTED = 10               # All test cases passed
    WRONG_ANSWER = 20           # Output didn't match expected
    MEMORY_LIMIT_EXCEEDED = 30  # Sandbox killed for memory
    TIME_LIMIT_EXCEEDED = 40    # Sandbox killed for timeout
    OUTPUT_LIMIT_EXCEEDED = 50  # stdout was too large
    COMPILE_ERROR = 60          # Compilation failed
    RUNTIME_ERROR = 70          # Process exited with non-zero code
    INTERNAL_ERROR = 80         # Something broke on our side
    UNSUPPORTED_LANGUAGE = 90   # Language not in allowed list


class TestStatus(IntEnum):
    """Per-test-case status."""
    PASSED = 1
    FAILED = 2
    TIMEOUT = 3
    MEMORY_EXCEED = 4
    RUNTIME_ERROR = 5
    OUTPUT_LIMIT = 6
    ERROR = 7


# ---------------------------------------------------------------------------
# SQS message — what arrives from your backend
# ---------------------------------------------------------------------------

@dataclass
class ExecutionRequest:
    """
    Represents one code-run request received from SQS.

    Fields:
        job_id:       Unique identifier for this execution
        language:     "python", "java", or "cpp"
        user_code:    The student's solution (plain text, after base64 decode)
        driver_code:  The test harness that imports/calls the solution
        task:         "RUN_CODE" or "SUBMIT_CODE"
        time_limit:   Seconds allowed per test case
        memory_limit: MB allowed per sandbox
        test_cases:   List of {"input": "...", "expected_output": "..."}
    """
    job_id: str
    language: str
    user_code: str
    driver_code: str
    task: str
    time_limit: int
    memory_limit: int
    test_cases: List[Dict[str, str]] = field(default_factory=list)
    receipt_handle: str = ""  # SQS needs this to delete the message later


# ---------------------------------------------------------------------------
# Results — what we produce
# ---------------------------------------------------------------------------

@dataclass
class TestCaseResult:
    """Result of running one test case."""
    index: int                            # 0-based test case number
    status: TestStatus                    # PASSED, FAILED, TIMEOUT, etc.
    input_data: str = ""                  # The input we fed
    expected_output: str = ""             # What the correct answer is
    actual_output: str = ""               # What the code produced (last line)
    std_output: str = ""                  # Debug output (everything except last line)
    stderr: str = ""
    runtime_ms: int = 0                   # How long the code ran (milliseconds)
    memory_mb: int = 0                    # Peak memory used (MB)
    error_message: Optional[str] = None           # Detailed message about the error if any

@dataclass
class ExecutionReport:
    """
    Final report for one job. This is what gets saved to the output directory
    and what your backend reads to update the submission status.
    """
    execution_id: str
    language: str
    task: str                              # RUN_CODE or SUBMIT_CODE
    status_code: int                       # One of StatusCode values
    status_msg: str                        # Human-readable status
    total_testcases: int = 0
    total_correct: int = 0
    test_results: Optional[List[TestCaseResult]] = None  # Present only for RUN_CODE task
    failed_tescase: Optional[Dict[str, str]] = None    # Present only for SUBMIT_CODE task 
    compile_error: Optional[str] = None    # Present only if compilation failed
    runtime_error: Optional[str] = None    # Present only if runtime error
    internal_error: Optional[str] = None   # Present only if we had a bug
    created_at: float = field(default_factory=time.time)  # Unix timestamp

    def to_json(self) -> str:
        """Serialize to JSON string for storage."""
        data = asdict(self)
        return json.dumps(data, indent=2)

    def save_to_file(self, path: str) -> None:
        """Write this report as a JSON file."""
        with open(path, "w") as f:
            f.write(self.to_json())


# ---------------------------------------------------------------------------
# Parsing — convert raw SQS message body into an ExecutionRequest
# ---------------------------------------------------------------------------

def safe_base64_decode(encoded: str) -> str:
    """
    Decode a base64 string, fixing padding if necessary.

    Base64 strings must be a multiple of 4 characters long.
    Some encoders omit the trailing '=' padding characters.
    This function adds them back before decoding.
    """
    # Add missing padding: base64 needs length to be multiple of 4
    missing_padding = len(encoded) % 4
    if missing_padding:
        encoded += "=" * (4 - missing_padding)

    return base64.b64decode(encoded).decode("utf-8")


def parse_sqs_message(body: str, receipt_handle: str = "") -> ExecutionRequest:
    """
    Parse the JSON body of an SQS message into an ExecutionRequest.

    Expected JSON format from your Java/Kotlin backend:
    {
        "jobId": "abc-123",
        "language": "python",
        "userCode": "<base64>",
        "driverCode": "<base64>",
        "task": "RUN_CODE",
        "time_limit": 5,
        "memory_limit": 50,
        "testCases": [
            {"input": "5\n3 4", "expectedOutput": "7"}
        ]
    }
    """

    data = json.loads(body)
    
    return ExecutionRequest(
        job_id=data["jobId"],
        language=data["language"].strip().lower(),
        user_code=safe_base64_decode(data["userCode"]),
        driver_code=safe_base64_decode(data["driverCode"]),
        task=data.get("task", "RUN_CODE"),
        time_limit=data.get("time_limit", 5),
        memory_limit=data.get("memory_limit", 50),
        test_cases=data.get("testCases", []),
        receipt_handle=receipt_handle,
    )

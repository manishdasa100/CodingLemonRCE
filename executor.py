"""
Executor — orchestrates the full lifecycle of a code execution job.

Flow:
    1. Create temp directory
    2. Write code files
    3. Compile (if needed)
    4. Run each test case in a fresh nsjail sandbox
    5. Collect results into an ExecutionReport
    6. Save report to output directory
    7. Cleanup temp directory
"""
import os
import shutil
import logging
from typing import List

from config import WorkerConfig
from models import (
    ExecutionRequest,
    ExecutionReport,
    TestCaseResult,
    StatusCode,
    TestStatus,
)
from sandbox import NsjailSandbox, SandboxResult
from languages import get_language

logger = logging.getLogger(__name__)


class Executor:
    """Runs a single ExecutionRequest and produces an ExecutionReport."""

    def __init__(self, config: WorkerConfig):
        self.config = config
        self.sandbox = NsjailSandbox(config.sandbox)

    async def execute(self, request: ExecutionRequest) -> ExecutionReport:
        """
        Execute a code-run request end-to-end.

        This is the main entry point called by the dispatcher.
        """
        job_id = request.job_id
        logger.info("[%s] Starting execution: language=%s, task=%s",
                     job_id, request.language, request.task)

        # ---- Step 0: Validate language ----
        lang = get_language(request.language)
        if lang is None:
            logger.warning("[%s] Unsupported language: %s", job_id, request.language)
            return self._error_report(
                request,
                StatusCode.UNSUPPORTED_LANGUAGE,
                f"Language '{request.language}' is not supported",
            )

        # ---- Step 1: Create temp directory ----
        exec_dir = os.path.join(self.config.execution.temp_dir, f"exec_{job_id}")
        code_dir = os.path.join(exec_dir, "code")

        try:
            os.makedirs(code_dir, exist_ok=True)
            # ---- Step 2: Write code files ----
            logger.info("[%s] Writing code files", job_id)
            lang.write_files(code_dir, request.user_code, request.driver_code)

            # ---- Step 3: Compile ----
            logger.info("[%s] Compiling (%s)", job_id, lang.name)
            compile_result = lang.compile(
                code_dir, self.config.execution.compilation_timeout
            )

            if not compile_result.success:
                logger.info("[%s] Compilation failed:\n%s", job_id, compile_result.error_message)
                report = self._error_report(
                    request,
                    StatusCode.COMPILE_ERROR,
                    "Compilation error",
                )
                report.compile_error = compile_result.error_message
                return report

            # ---- Step 4: Clamp resource limits ----
            time_limit = min(
                request.time_limit,
                self.config.execution.default_time_limit,
            )
            memory_limit = min(
                request.memory_limit,
                self.config.execution.default_memory_limit,
            )

            # ---- Step 5: Run test cases ----
            command = lang.run_command()
            test_results = await self._run_test_cases(
                job_id=job_id,
                command=command,
                code_dir=code_dir,
                test_cases=request.test_cases,
                time_limit=time_limit,
                memory_limit=memory_limit,
                stop_on_failure=(request.task == "SUBMIT_CODE"),
            )

            # ---- Step 6: Build report ----
            report = self._build_report(request, test_results)

            logger.info(
                "[%s] Execution complete: status=%s, passed=%d/%d",
                job_id, report.status_msg, report.total_correct, report.total_testcases,
            )
            return report

        except Exception as e:
            logger.exception("[%s] Internal error during execution", job_id)
            report = self._error_report(
                request,
                StatusCode.INTERNAL_ERROR,
                "Internal execution error",
            )
            report.internal_error = str(e)
            return report

        finally:
            # ---- Step 7: Cleanup ----
            try:
                shutil.rmtree(exec_dir, ignore_errors=True)
            except Exception:
                pass

    async def _run_test_cases(
        self,
        job_id: str,
        command: List[str],
        code_dir: str,
        test_cases: List[dict],
        time_limit: int,
        memory_limit: int,
        stop_on_failure: bool,
    ) -> List[TestCaseResult]:
        """
        Run each test case in a fresh sandbox and collect results.

        Each test case gets its own nsjail invocation. This means:
        - Clean state: no leftover variables, files, or memory between tests
        - Independent timeouts: one slow test doesn't eat into another's time
        - Isolation: if one test crashes, the others still run

        For SUBMIT_CODE tasks, we stop at the first failure (the student
        only sees the first failing test case, as is convention on platforms
        like LeetCode).

        For RUN_CODE tasks, we run ALL test cases regardless of failures
        (the student wants to see all outputs).
        """
        results = []

        for i, test_case in enumerate(test_cases):
            input_data = test_case.get("input", "")
            expected_output = test_case.get("expectedOutput", "").strip()

            logger.debug("[%s] Running test case %d/%d", job_id, i + 1, len(test_cases))

            # Run in sandbox
            sandbox_result = await self.sandbox.run(
                command=command,
                code_dir=code_dir,
                stdin_data=input_data,
                time_limit=time_limit,
                memory_limit=memory_limit,
            )

            # Convert sandbox result to test case result
            tc_result = self._evaluate_test_case(
                index=i,
                sandbox_result=sandbox_result,
                input_data=input_data,
                expected_output=expected_output,
            )
            results.append(tc_result)

            # For SUBMIT_CODE: stop on first failure
            if stop_on_failure and tc_result.status != TestStatus.PASSED:
                logger.info("[%s] Test case %d failed, stopping (SUBMIT_CODE)", job_id, i)
                break

        return results

    def _evaluate_test_case(
        self,
        index: int,
        sandbox_result: SandboxResult,
        input_data: str,
        expected_output: str,
    ) -> TestCaseResult:
        """
        Look at what the sandbox returned and classify the outcome.

        Output parsing convention (same as existing CodeExecutor):
        - The LAST non-empty line of stdout is the "answer"
        - Everything before it is "debug output" (print statements for debugging)
        """
        # Handle sandbox-level failures first
        if sandbox_result.timed_out:
            return TestCaseResult(
                index=index,
                status=TestStatus.TIMEOUT,
                input_data=input_data,
                expected_output=expected_output,
                runtime_ms=sandbox_result.runtime_ms,
            )

        if sandbox_result.oom_killed:
            return TestCaseResult(
                index=index,
                status=TestStatus.MEMORY_EXCEED,
                input_data=input_data,
                expected_output=expected_output,
                runtime_ms=sandbox_result.runtime_ms
            )

        # Check for runtime errors (non-zero exit, signals)
        if sandbox_result.exit_code != 0:
            stderr = sandbox_result.stderr.strip() if sandbox_result.stderr else ""

            # nsjail failed to launch iteself/nsjail failed to launch the child process
            if sandbox_result.exit_code in (-1, 255):
                logger.error(
                    "Sandbox failed to execute (exit_code=%d) stderr=%r nsjail_log=%r",
                    sandbox_result.exit_code, stderr, sandbox_result.nsjail_log,
                )
                return TestCaseResult(
                    index=index,
                    status=TestStatus.ERROR,
                    input_data=input_data,
                    expected_output=expected_output,
                    stderr=stderr,
                    error_message="Sandbox failed to execute",
                    runtime_ms=sandbox_result.runtime_ms
                )

            # User code crashed — signal kill or non-zero exit
            status = TestStatus.RUNTIME_ERROR
            if sandbox_result.signal:
                signal_names = {
                    9: "SIGKILL", 11: "SIGSEGV (Segmentation fault)",
                    6: "SIGABRT", 8: "SIGFPE (Floating point exception)",
                }
                sig_name = signal_names.get(
                    sandbox_result.signal, f"Signal {sandbox_result.signal}"
                )
                error_msg = f"Process killed by {sig_name}"
            else:
                error_msg = stderr or "Runtime error. Check stderr."

            return TestCaseResult(
                index=index,
                status=status,
                input_data=input_data,
                expected_output=expected_output,
                std_output=sandbox_result.stdout,
                stderr=stderr,
                error_message=error_msg,
                runtime_ms=sandbox_result.runtime_ms
            )

        # Process exited cleanly — parse the output
        stdout = sandbox_result.stdout

        # Check output size limits
        if len(stdout.encode("utf-8")) > self.config.execution.max_output_bytes:
            return TestCaseResult(
                index=index,
                status=TestStatus.OUTPUT_LIMIT,
                input_data=input_data,
                expected_output=expected_output,
                runtime_ms=sandbox_result.runtime_ms
            )

        # Split into lines, find the answer (last non-empty line)
        lines = stdout.rstrip("\n").split("\n")
        non_empty_lines = [line for line in lines if line.strip()]

        if not non_empty_lines:
            actual_output = ""
            std_output = ""
        else:
            actual_output = non_empty_lines[-1].strip()
            # Everything except the last non-empty line is debug output
            debug_lines = non_empty_lines[:-1]
            std_output = "\n".join(debug_lines)

        # Compare output with expected
        if expected_output and actual_output == expected_output:
            status = TestStatus.PASSED
        elif not expected_output:
            # No expected output provided (RUN_CODE mode) — just report output
            status = TestStatus.PASSED
        else:
            status = TestStatus.FAILED

        return TestCaseResult(
            index=index,
            status=status,
            input_data=input_data,
            expected_output=expected_output,
            actual_output=actual_output,
            std_output=std_output,
            runtime_ms=sandbox_result.runtime_ms
        )

    def _resolve_status(self, tc: TestCaseResult) -> tuple:
        """
        Map a single TestCaseResult status to (StatusCode, status_msg,
        runtime_error, internal_error).
        """
        if tc.status == TestStatus.ERROR:
            return (StatusCode.INTERNAL_ERROR, "Internal Error", None, tc.error_message)
        elif tc.status == TestStatus.RUNTIME_ERROR:
            return (StatusCode.RUNTIME_ERROR, "Runtime Error", tc.error_message, None)
        elif tc.status == TestStatus.TIMEOUT:
            return (StatusCode.TIME_LIMIT_EXCEEDED, "Time Limit Exceeded", None, None)
        elif tc.status == TestStatus.MEMORY_EXCEED:
            return (StatusCode.MEMORY_LIMIT_EXCEEDED, "Memory Limit Exceeded", None, None)
        elif tc.status == TestStatus.OUTPUT_LIMIT:
            return (StatusCode.OUTPUT_LIMIT_EXCEEDED, "Output Limit Exceeded", None, None)
        else:
            return (StatusCode.WRONG_ANSWER, "Wrong Answer", None, None)
    def _build_report(
        self,
        request: ExecutionRequest,
        test_results: List[TestCaseResult],
    ) -> ExecutionReport:
        """
        Aggregate test case results into a final ExecutionReport.

        SUBMIT_CODE: status is driven by the first failing test case.
        RUN_CODE:    all test cases run — status follows precedence:
                     INTERNAL_ERROR > RUNTIME_ERROR > TIME_LIMIT_EXCEEDED
                     > MEMORY_LIMIT_EXCEEDED > OUTPUT_LIMIT_EXCEEDED > WRONG_ANSWER
        """
        total = len(request.test_cases)
        passed = sum(1 for r in test_results if r.status == TestStatus.PASSED)

        is_submmit_code = request.task == "SUBMIT_CODE"

        runtime_error = None
        internal_error = None
        first_failure = None

        if total == 0:
            logger.error("[%s] No test cases in request", request.job_id)
            status_code = StatusCode.INTERNAL_ERROR
            status_msg = "No test cases"
        elif passed == total:
            status_code = StatusCode.ACCEPTED   
            status_msg = "Accepted"
        elif is_submmit_code:
            # Stopped at first failure — use that to determine status
            first_failure = next(r for r in test_results if r.status != TestStatus.PASSED)
            status_code, status_msg, runtime_error, internal_error = self._resolve_status(first_failure)
        else:
            # RUN_CODE — all tests ran, pick worst by precedence
            precedence = [
                TestStatus.ERROR,
                TestStatus.RUNTIME_ERROR,
                TestStatus.TIMEOUT,
                TestStatus.MEMORY_EXCEED,
                TestStatus.OUTPUT_LIMIT,
                TestStatus.FAILED,
            ]
            failures = [r for r in test_results if r.status != TestStatus.PASSED]
            dominant = min(
                failures,
                key=lambda r: precedence.index(r.status) if r.status in precedence else len(precedence)
            )
            status_code, status_msg, runtime_error, internal_error = self._resolve_status(dominant)

        max_runtime_ms = max((r.runtime_ms for r in test_results), default=0)
        max_memory_mb = max((r.memory_mb for r in test_results), default=0)

        return ExecutionReport(
            execution_id=request.job_id,
            language=request.language,
            task=request.task,
            status_code=status_code,
            status_msg=status_msg,
            runtime_error=runtime_error,
            internal_error=internal_error,
            total_testcases=total,
            total_correct=passed,
            runtime_ms=max_runtime_ms,
            memory_mb=max_memory_mb,
            failed_testcase=first_failure if is_submmit_code else None,
            test_results=test_results if not is_submmit_code else None
        )

    def _error_report(
        self,
        request: ExecutionRequest,
        status_code: StatusCode,
        status_msg: str,
    ) -> ExecutionReport:
        """Create a report for pre-execution failures (unsupported lang, compile error)."""
        return ExecutionReport(
            execution_id=request.job_id,
            language=request.language,
            task=request.task,
            status_code=status_code,
            status_msg=status_msg,
        )

    

"""
Sandbox wrapper — invokes nsjail to run untrusted code.

This module builds the nsjail command line and runs it as a subprocess.
It does NOT know anything about languages, test cases, or reports —
it just takes a command, runs it in a sandbox, and returns what happened.
"""
import asyncio
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from typing import List, Optional

from config import SandboxConfig

logger = logging.getLogger(__name__)


@dataclass
class SandboxResult:
    """What came out of the sandbox."""
    stdout: str            # Everything the sandboxed process printed to stdout
    stderr: str            # Everything printed to stderr (runtime errors, etc.)
    nsjail_log: str
    exit_code: int         # 0 = clean exit, non-zero = error or killed
    timed_out: bool        # True if nsjail killed the process for exceeding time_limit
    oom_killed: bool       # True if killed for exceeding memory limit
    runtime_ms: int        # Wall-clock time in milliseconds
    signal: Optional[int]  # Signal number if killed by signal (e.g., 9=SIGKILL, 11=SIGSEGV)


class NsjailSandbox:
    """
    Wraps nsjail invocation.

    Usage:
        sandbox = NsjailSandbox(config)
        result = await sandbox.run(
            command=["python3", "/code/main.py"],
            code_dir="/tmp/exec_abc123/code",
            stdin_data="5\n3 4\n",
            time_limit=5,
            memory_limit=50,
        )
    """

    def __init__(self, config: SandboxConfig):
        self.config = config

    def _build_command(
        self,
        command: List[str],
        code_dir: str,
        time_limit: int,
        memory_limit: int,
        log_file: str = "",
    ) -> List[str]:
        """
        Build the full nsjail command line.

        nsjail reads its base config from sandbox_minimal.cfg, but we
        override per-execution settings (time limit, memory limit, code
        directory) via command-line flags. CLI flags take precedence
        over config file values.

        log_file: if non-empty, nsjail's own log output is written to this
                  path instead of stderr, keeping stderr clean for the
                  sandboxed process's actual output.
        """
        memory_bytes = memory_limit * 1024 * 1024  # Convert MB to bytes

        nsjail_cmd = [
            "sudo", self.config.nsjail_path,
            "--config", self.config.config_path,

            # Redirect nsjail's own logs to a file, not stderr.
            # --log_file survives the sudo boundary unlike --log_fd.
            "--log", log_file,

            # Override time/memory per request
            "--time_limit", str(time_limit),
            "--cgroup_mem_max", str(memory_bytes),

            # Bind-mount the code directory into the sandbox at /code (read-only)
            "--bindmount_ro", f"{code_dir}:/code",

            # Separator: everything after "--" is the command to run inside the sandbox
            "--",
        ]

        # Append the actual command (e.g., ["python3", "/code/main.py"])
        nsjail_cmd.extend(command)

        return nsjail_cmd

    async def run(
        self,
        command: List[str],
        code_dir: str,
        stdin_data: str = "",
        time_limit: int = 5,
        memory_limit: int = 50,
    ) -> SandboxResult:
        """
        Run a command inside an nsjail sandbox.

        Args:
            command:      The command to execute inside the sandbox
                          e.g. ["python3", "/code/main.py"]
            code_dir:     Host path to the code directory (bind-mounted to /code)
            stdin_data:   Input to feed to the process via stdin
            time_limit:   Seconds before nsjail kills the process
            memory_limit: MB memory limit

        Returns:
            SandboxResult with stdout, stderr, exit code, and timing info
        """
        # Create a temp file for nsjail's own log output so it doesn't
        # pollute stderr. We use a file path instead of --log_fd because
        # sudo sanitises inherited file descriptors before execing nsjail,
        # so a pipe fd would never reach nsjail. A file path survives sudo.
        log_file = tempfile.NamedTemporaryFile(
            prefix="nsjail_log_", suffix=".txt", delete=False
        )
        log_path = log_file.name
        log_file.close()
        # Make the log file world-writable so nsjail (running as root via
        # sudo) can write to it regardless of the owning user.
        os.chmod(log_path, 0o666)

        nsjail_cmd = self._build_command(
            command,
            code_dir,
            time_limit,
            memory_limit,
            log_path,
        )

        logger.debug("nsjail command: %s", " ".join(nsjail_cmd))

        start_time = time.monotonic()

        try:
            # asyncio.create_subprocess_exec runs nsjail as a child process
            # without blocking the event loop. Other sandboxes can run
            # concurrently while this one is executing.
            process = await asyncio.create_subprocess_exec(
                *nsjail_cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            # Feed stdin and wait for the process to finish.
            # We add a generous buffer on top of the nsjail time_limit
            # because nsjail itself needs a moment to set up namespaces
            # and tear down. If this outer timeout fires, something is
            # very wrong (nsjail hung).
            outer_timeout = time_limit + 10

            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                process.communicate(input=stdin_data.encode("utf-8")),
                timeout=outer_timeout,
            )

            elapsed_ms = int((time.monotonic() - start_time) * 1000)

            # Read nsjail's log file. nsjail has already exited at this
            # point so the file is fully written.
            with open(log_path, "r", errors="replace") as f:
                nsjail_log = f.read()
            logger.debug("nsjail log: %s", nsjail_log)

            stdout = stdout_bytes.decode("utf-8", errors="replace")
            stderr = stderr_bytes.decode("utf-8", errors="replace")
            exit_code = process.returncode or 0

            # Detect how the process ended — check nsjail's own log for signals
            # since stderr is now clean of nsjail output
            timed_out = self._is_timeout(exit_code, nsjail_log)
            oom_killed = self._is_oom(exit_code, nsjail_log)
            signal = self._extract_signal(exit_code)

            return SandboxResult(
                stdout=stdout,
                stderr=stderr,
                nsjail_log=nsjail_log,
                exit_code=exit_code,
                timed_out=timed_out,
                oom_killed=oom_killed,
                runtime_ms=elapsed_ms,
                signal=signal,
            )

        except asyncio.TimeoutError:
            # The OUTER timeout fired — nsjail itself hung
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            logger.error("nsjail process hung (outer timeout after %dms)", elapsed_ms)

            # Kill the nsjail process
            try:
                process.kill()
                await process.wait()
            except Exception:
                pass

            return SandboxResult(
                stdout="",
                stderr="Sandbox process timed out (nsjail hung)",
                nsjail_log="",
                exit_code=-1,
                timed_out=False,
                oom_killed=False,
                runtime_ms=elapsed_ms,
                signal=None,
            )

        finally:
            try:
                os.unlink(log_path)
            except FileNotFoundError:
                pass

    def _is_timeout(self, exit_code: int, stderr: str) -> bool:
        """
        Detect if nsjail killed the process for exceeding the time limit.

        nsjail logs "run time >= time limit" and sends SIGKILL (exit 137)
        to the sandboxed process. We match on the log string since exit 137
        is also used by OOM kills.
        """
        if "time limit" in stderr.lower():
            return True
        return False

    def _is_oom(self, exit_code: int, nsjail_log: str) -> bool:
        """
        Detect if the process was killed for exceeding the memory limit.

        The cgroup OOM killer sends SIGKILL (signal 9) and nsjail logs
        "oom" or "memory cgroup" in its output.
        """
        log_lower = nsjail_log.lower()
        if "oom" in log_lower or "out of memory" in log_lower:
            return True
        if "memory cgroup" in log_lower:
            return True
        return False

    def _extract_signal(self, exit_code: int) -> Optional[int]:
        """
        If the process was killed by a signal, extract the signal number.

        Convention: exit code > 128 means killed by signal (exit_code - 128).
        For example: 137 = SIGKILL (9), 139 = SIGSEGV (11), 143 = SIGTERM (15).
        """
        if exit_code > 128:
            return exit_code - 128
        return None

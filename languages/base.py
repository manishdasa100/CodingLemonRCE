"""
Abstract base class that every language handler must implement.

A language handler is responsible for three things:
1. Writing code files to the temp directory in the right layout
2. Compiling those files (if the language requires compilation)
3. Telling the sandbox what command to run inside nsjail
"""
from abc import ABC, abstractmethod
from typing import List, Optional
from dataclasses import dataclass


@dataclass
class CompileResult:
    """Outcome of the compilation step."""
    success: bool
    error_message: str = ""  # Only populated if success=False


class BaseLanguage(ABC):
    """
    Interface that every language must implement.

    The executor calls these methods in order:
        1. write_files(...)   — put code files on disk
        2. compile(...)       — compile if needed (Python returns success immediately)
        3. run_command(...)   — get the command nsjail should execute
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable language name, e.g. 'Python'."""
        ...

    @abstractmethod
    def write_files(
        self,
        code_dir: str,
        user_code: str,
        driver_code: str,
    ) -> None:
        """
        Write the source code files to `code_dir`.

        For Python this means creating solution.py and main.py.
        For Java this means creating Solution.java and Main.java.
        For C++ this means creating solution.cpp and main.cpp.

        Args:
            code_dir:    Absolute path to the temp directory for code files
            user_code:   The student's solution (plain text)
            driver_code: The test harness (plain text)
        """
        ...

    @abstractmethod
    def compile(self, code_dir: str, timeout: int) -> CompileResult:
        """
        Compile the source files. Called on the HOST, not inside nsjail.

        For interpreted languages (Python), just return CompileResult(success=True).
        For compiled languages, run the compiler and return the result.

        Args:
            code_dir: Directory containing the source files
            timeout:  Max seconds for compilation

        Returns:
            CompileResult with success=True or error_message set
        """
        ...

    @abstractmethod
    def run_command(self, code_dir: str) -> List[str]:
        """
        Return the command that nsjail should execute inside the sandbox.

        This is a list of strings, e.g.:
            ["python3", "/code/main.py"]
            ["java", "-cp", "/code", "Main"]
            ["/code/main"]

        The returned paths should be sandbox-internal paths (e.g. /code/...),
        NOT host paths, because this command runs INSIDE nsjail where code_dir
        is bind-mounted to /code.

        Args:
            code_dir: The host path to the code directory (for reference only)

        Returns:
            List of strings forming the command to execute
        """
        ...

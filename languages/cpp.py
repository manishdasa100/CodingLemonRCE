import os
import subprocess
from typing import List

from languages.base import BaseLanguage, CompileResult


class CppLanguage(BaseLanguage):
    """
    Handler for C++ code execution.

    File layout inside code_dir:
        solution.cpp  — the student's code
        main.cpp      — the driver/test harness (includes solution.cpp)
        main          — compiled executable (after compilation)

    Compilation runs on the HOST using g++, not inside nsjail.
    Only the compiled binary is needed at runtime.
    """

    @property
    def name(self) -> str:
        return "C++"

    def write_files(
        self,
        code_dir: str,
        user_code: str,
        driver_code: str,
    ) -> None:
        solution_path = os.path.join(code_dir, "solution.cpp")
        with open(solution_path, "w") as f:
            f.write(user_code)

        main_path = os.path.join(code_dir, "main.cpp")
        with open(main_path, "w") as f:
            f.write(driver_code)

    def compile(self, code_dir: str, timeout: int) -> CompileResult:
        """
        Run g++ to compile into a single executable.

        The driver (main.cpp) typically does #include "solution.cpp"
        so we only need to compile main.cpp — it pulls in the solution.
        """
    
        try:
            result = subprocess.run(
                [
                    "g++",
                    "-std=c++17",
                    "-O2",                # Optimize for speed (students expect fast code)
                    "-o", "main",
                    "main.cpp",
                ],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=code_dir
            )

            if result.returncode != 0:
                return CompileResult(
                    success=False,
                    error_message=result.stderr.strip(),
                )

            return CompileResult(success=True)

        except subprocess.TimeoutExpired:
            return CompileResult(
                success=False,
                error_message="Compilation timed out",
            )
        except FileNotFoundError:
            return CompileResult(
                success=False,
                error_message="C++ compiler (g++) not found on this system",
            )

    def run_command(self) -> List[str]:
        # The compiled binary is at /code/main inside the sandbox
        return ["/code/main"]

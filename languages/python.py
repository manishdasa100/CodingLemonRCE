import os
from typing import List

from languages.base import BaseLanguage, CompileResult


class PythonLanguage(BaseLanguage):
    """
    Handler for Python code execution.

    File layout inside code_dir:
        solution.py   — the student's code
        main.py       — the driver/test harness that imports solution

    No compilation step needed — Python is interpreted.
    """

    @property
    def name(self) -> str:
        return "Python"

    def write_files(
        self,
        code_dir: str,
        user_code: str,
        driver_code: str,
    ) -> None:
        # Write the student's solution
        solution_path = os.path.join(code_dir, "solution.py")
        with open(solution_path, "w") as f:
            f.write(user_code)

        # Write the driver code that imports and tests the solution
        main_path = os.path.join(code_dir, "main.py")
        with open(main_path, "w") as f:
            f.write(driver_code)

    def compile(self, code_dir: str, timeout: int) -> CompileResult:
        # Python doesn't need compilation
        return CompileResult(success=True)

    def run_command(self) -> List[str]:
        # Inside nsjail, the code_dir is mounted at /code
        return ["/usr/bin/python3", "/code/main.py"]

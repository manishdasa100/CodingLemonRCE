import os
import subprocess
from typing import List

from languages.base import BaseLanguage, CompileResult


class JavaLanguage(BaseLanguage):
    """
    Handler for Java code execution.

    File layout inside code_dir:
        Solution.java  — the student's code
        Main.java      — the driver/test harness
        *.class        — compiled bytecode (after compilation)

    Compilation runs on the HOST using javac, not inside nsjail.
    Only the compiled .class files are needed at runtime.
    """

    @property
    def name(self) -> str:
        return "Java"

    def write_files(
        self,
        code_dir: str,
        user_code: str,
        driver_code: str,
    ) -> None:
        solution_path = os.path.join(code_dir, "Solution.java")
        with open(solution_path, "w") as f:
            f.write(user_code)

        main_path = os.path.join(code_dir, "Main.java")
        with open(main_path, "w") as f:
            f.write(driver_code)

    def compile(self, code_dir: str, timeout: int) -> CompileResult:
        """
        Run javac to compile both Java files.

        We compile on the host because:
        1. javac needs file system access that sandboxing complicates
        2. The compiler itself is a trusted binary (not user code)
        3. Compilation errors are just text — no security risk
        """
        try:
            result = subprocess.run(
                [
                    "javac",
                    "Main.java",
                    "Solution.java",
                ],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=code_dir
            )

            if result.returncode != 0:
                # javac prints errors to stderr
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
                error_message="Java compiler (javac) not found on this system",
            )

    def run_command(self) -> List[str]:
        # -Xms8m  : start with 8 MB heap (don't reserve 256 MB upfront)
        # -Xmx32m : cap heap at 32 MB (fits within the 50 MB cgroup limit)
        # -cp /code : look for .class files in /code
        return [
            "/usr/lib/jvm/java-11-openjdk-amd64/bin/java",
            "-Xms8m", "-Xmx32m",
            "-XX:MaxMetaspaceSize=64m",
            "-cp", "/code",
            "Main",
        ]

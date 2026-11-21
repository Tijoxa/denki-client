"""Run the denki-client checks at the end of a Codex turn."""

import json
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CHECKS = (
    ("uvx", "ruff", "check"),
    ("uvx", "ty", "check"),
    ("cargo", "fmt", "--all", "--", "--check"),
    ("cargo", "clippy", "--all-targets"),
)
TEST_REVIEW = (
    "Review the tests you added during this turn, including inline Rust tests. "
    "Remove tests that duplicate existing coverage, assert only mock behavior, "
    "or lack a meaningful assertion about the code's behavior. "
    "Preserve pre-existing tests, user edits, requested coverage, and tests "
    "that catch a distinct regression. Finish the turn after this review."
)


def needs_test_review() -> bool:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        return False
    if payload.get("stop_hook_active"):
        return False

    try:
        result = subprocess.run(
            (
                "git",
                "--no-optional-locks",
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
                "--",
                "tests",
                ":(glob)**/test_*.py",
                ":(glob)**/*_test.py",
                ":(glob)**/*.rs",
            ),
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
        )
    except OSError:
        return False
    return result.returncode == 0 and bool(result.stdout)


def main() -> None:
    if needs_test_review():
        print(json.dumps({"decision": "block", "reason": TEST_REVIEW}))
        return

    failures = []
    for command in CHECKS:
        env = os.environ.copy()
        if command[:2] == ("cargo", "clippy"):
            # The extension uses abi3, so Clippy can run without a Python interpreter.
            env["PYO3_NO_PYTHON"] = "1"

        label = " ".join(command)
        try:
            result = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                check=False,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                encoding="utf-8",
                errors="replace",
            )
        except OSError as error:
            failures.append(f"{label} could not start: {error}")
            continue

        if result.returncode:
            output = result.stdout.strip()
            if len(output) > 1200:
                output = f"{output[:850]}\n...\n{output[-300:]}"
            failures.append(f"{label} exited with status {result.returncode}\n{output}")

    response = {"continue": True}
    if failures:
        response["systemMessage"] = "denki-client checks failed\n\n" + "\n\n".join(failures)
    print(json.dumps(response))


if __name__ == "__main__":
    main()

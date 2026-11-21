"""Run the denki-client checks at the end of a Codex turn."""

import json
import os
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CHECKS = (
    ("uvx", "ruff", "check"),
    ("uvx", "ty", "check"),
    ("cargo", "fmt", "--all", "--", "--check"),
    ("cargo", "clippy", "--all-targets"),
)


def main() -> None:
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

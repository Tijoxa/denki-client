"""Check denki-client formatting at the end of a Codex turn."""

import json
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CHECKS = (
    ("uvx", "ruff", "format", "--check", "--extend-exclude=*.md"),
    ("cargo", "fmt", "--all", "--", "--check"),
)


def main() -> None:
    failures = []
    for command in CHECKS:
        label = " ".join(command)
        try:
            result = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                check=False,
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
        response["systemMessage"] = "denki-client formatting checks failed\n\n" + "\n\n".join(failures)
    print(json.dumps(response))


if __name__ == "__main__":
    main()

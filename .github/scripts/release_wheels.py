"""Publish the wheels from one workflow run as GitHub Release assets."""

import json
import os
import subprocess
import sys
import tomllib
from email import message_from_bytes
from fnmatch import fnmatchcase
from pathlib import Path
from zipfile import ZipFile

PLATFORMS = ("win_amd64", "manylinux_2_28_x86_64", "macosx_*_arm64", "macosx_*_x86_64")


def gh(*args: str) -> str:
    return subprocess.check_output(["gh", *args], encoding="utf-8").strip()


def release_for_tag(repository: str, tag: str) -> dict | None:
    pages = json.loads(gh("api", "--paginate", "--slurp", f"repos/{repository}/releases?per_page=100"))
    return next((release for page in pages for release in page if release["tag_name"] == tag), None)


def wheel_assets(project: dict) -> list[Path]:
    name, version = project["name"], project["version"]
    prefix = f"{name.replace('-', '_')}-{version}-"
    wheels = sorted(Path("dist").glob(f"{prefix}*.whl"))
    platforms = [wheel.stem.rsplit("-", 1)[-1] for wheel in wheels]
    if len(wheels) != len(PLATFORMS) or any(
        sum(fnmatchcase(platform, pattern) for platform in platforms) != 1 for pattern in PLATFORMS
    ):
        raise ValueError(f"Expected one wheel for each release platform: {wheels}")

    for wheel in wheels:
        with ZipFile(wheel) as archive:
            metadata_name = f"{name.replace('-', '_')}-{version}.dist-info/METADATA"
            metadata = message_from_bytes(archive.read(metadata_name))
        if (
            metadata["Name"] != name
            or metadata["Version"] != version
            or metadata["Requires-Python"] != project["requires-python"]
        ):
            raise ValueError(f"Unexpected distribution metadata in {wheel}")
    return wheels


def publish(repository: str, tag: str, release: dict | None, project: dict) -> None:
    if release is not None and not release["draft"]:
        print(f"Release {tag} is already published.")
        return

    commit = os.environ["GITHUB_SHA"]
    if release is not None and release["target_commitish"] != commit:
        raise ValueError(f"Draft {tag} targets {release['target_commitish']}, expected {commit}")
    tag_pages = json.loads(gh("api", "--paginate", "--slurp", f"repos/{repository}/tags?per_page=100"))
    tag_commit = next((ref["commit"]["sha"] for page in tag_pages for ref in page if ref["name"] == tag), None)
    if tag_commit is not None and tag_commit != commit:
        raise ValueError(f"Tag {tag} points to {tag_commit}, expected {commit}")

    wheels = wheel_assets(project)
    manifest = Path("dist/wheel-index.json")
    info = {field: project[field] for field in ("name", "version", "requires-python")}
    info["wheels"] = [wheel.name for wheel in wheels]
    manifest.write_text(json.dumps(info, indent=2) + "\n", encoding="utf-8")

    if release is None:
        gh(
            "release",
            "create",
            tag,
            "--repo",
            repository,
            "--draft",
            "--target",
            commit,
            "--title",
            f"{project['name']} {project['version']}",
            "--generate-notes",
        )
    gh("release", "upload", tag, *map(str, wheels), str(manifest), "--repo", repository, "--clobber")
    print(gh("release", "edit", tag, "--draft=false", "--repo", repository))


def main() -> None:
    if sys.argv[1:] not in (["plan"], ["publish"]):
        sys.exit("Usage: release_wheels.py plan|publish")

    repository = os.environ["GITHUB_REPOSITORY"]
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"]
    tag = f"{project['name']}-v{project['version']}"
    release = release_for_tag(repository, tag)
    if sys.argv[1] == "plan":
        build = "true" if release is None or release["draft"] else "false"
        with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as output:
            print(f"build={build}", file=output)
    else:
        publish(repository, tag, release, project)


if __name__ == "__main__":
    main()

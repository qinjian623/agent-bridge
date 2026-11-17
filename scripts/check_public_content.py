"""Check publishable files for common credentials and private example data, without printing matches."""

from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path

_PATTERNS = {
    "private key": re.compile(r"-----BEGIN (?:OPENSSH |RSA |EC |DSA )?PRIVATE KEY-----"),
    "credential": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{25,}|github_pat_[A-Za-z0-9_]{30,}|sk-(?:proj-)?[A-Za-z0-9_-]{30,})"),
    "home path": re.compile(r"(?:/home/|/Users/|[A-Za-z]:\\Users\\)([A-Za-z0-9_.-]+)"),
    "email": re.compile(r"\b[A-Za-z0-9_.+-]+@([A-Za-z0-9.-]+\.[A-Za-z]{2,})\b"),
    "session ID": re.compile(r"\b[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\b", re.I),
}
_EXAMPLE_USERS = {"user", "username", "worker", "alice", "bob", "example", "developer", "test"}
_EXAMPLE_DOMAINS = {"example.com", "example.org", "example.net", "localhost"}
_PRIVATE_FILES = {".env", "auth.json", "admin.token", "credentials.json"}


def issues(text: str) -> list[tuple[int, str]]:
    found = []
    for number, line in enumerate(text.splitlines(), 1):
        for category, pattern in _PATTERNS.items():
            for match in pattern.finditer(line):
                if category == "home path" and match[1].lower() in _EXAMPLE_USERS:
                    continue
                if category == "email" and match[0].startswith("pass@") and line[:match.start()].endswith("://user:"):
                    continue  # Synthetic user:pass URLs exercise credential rejection.
                if category == "email" and (
                    match[1].lower() in _EXAMPLE_DOMAINS
                    or match[1].lower().endswith((".test", ".invalid", ".example", ".example.com", ".example.org", ".example.net", ".noreply.github.com"))
                ):
                    continue
                if category == "session ID" and (
                    len(set(match[0].replace("-", "").lower())) <= 3
                    or match[0].lower().startswith("00000000-0000-7000-8000-0000000000")
                ):
                    continue
                found.append((number, category))
                break
    return found


def check(root: Path) -> int:
    names = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=root,
    ).decode().split("\0")
    count = 0
    for name in sorted(set(names) - {""}):
        path = root / name
        if not path.is_file():
            continue
        if path.name in _PRIVATE_FILES or re.search(r"\.(?:(?:sqlite\d*|db)(?:-(?:wal|shm|journal))?|pem|key|jsonl|mp4|webm)$", name, re.I):
            print(f"{name}: private runtime file must not be published")
            count += 1
            continue
        if path.stat().st_size > 5 * 1024 * 1024:
            print(f"{name}: file exceeds the content-check limit; review before publishing")
            count += 1
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for number, category in issues(text):
            print(f"{name}:{number}: possible {category}; use synthetic examples")
            count += 1
    print(f"Public content check: {count} finding(s). Values are never printed; this is not a security audit.")
    return int(bool(count))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    raise SystemExit(check(parser.parse_args().root))

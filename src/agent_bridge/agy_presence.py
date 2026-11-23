"""Read native AGY ownership before starting a closed conversation."""

from __future__ import annotations

import fcntl
import os
import sys
from pathlib import Path


def unsupported_platform_reason(platform_name=None):
    """Identify platforms without verified automatic-resume ownership checks."""
    platform_name = sys.platform if platform_name is None else platform_name
    if platform_name.startswith("linux"):
        return None
    platform_label = "macOS" if platform_name == "darwin" else platform_name
    # Darwin's F_GETLK reports pid -1 for flock; an open fd does not prove ownership.
    return (f"AGY automatic resume is unsupported on {platform_label}: "
            "process and native flock ownership cannot be verified safely")


def _lock_records(proc_root):
    records = []
    for line in (proc_root / "locks").read_text().splitlines():
        fields = line.split()
        if len(fields) < 6 or fields[1:4] != ["FLOCK", "ADVISORY", "WRITE"]:
            continue
        try:
            records.append((int(fields[4]), int(fields[5].rsplit(":", 1)[1])))
        except ValueError:
            continue
    return records


def _presence_sessions(pid, native_home, proc_root):
    """Resolve a process's native IDs from held locks, never from an open fd alone."""
    records = _lock_records(proc_root)
    inodes = {inode for owner, inode in records if owner == pid}
    found = set()
    directory = (native_home / "presence").resolve()
    for fd in (proc_root / str(pid) / "fd").iterdir():
        try:
            path = Path(os.readlink(fd))
            if path.parent.resolve() == directory and path.suffix == ".lock" and fd.stat().st_ino in inodes:
                found.add(path.stem)
        except FileNotFoundError:
            continue
    return found


def _target_lock_reason(native_id, native_home, proc_root, exclude_pid):
    path = native_home / "presence" / (native_id + ".lock")
    try:
        handle = path.open("rb")
    except FileNotFoundError:
        return None
    except OSError:
        return "Cannot inspect native AGY presence lock"
    with handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            if exclude_pid is not None:
                try:
                    owners = {pid for pid, inode in _lock_records(proc_root) if inode == path.stat().st_ino}
                    if owners == {exclude_pid} and native_id in _presence_sessions(exclude_pid, native_home, proc_root):
                        return None
                except OSError:
                    pass
            return "Native AGY presence lock is held"
        except OSError:
            return "Cannot inspect native AGY presence lock"
    return None


def inactive_reason(native_id, executable, *, native_home=None, proc_root=None, exclude_pid=None):
    """Return None only when no matching or unidentified AGY instance is active."""
    if not isinstance(native_id, str) or not native_id or Path(native_id).name != native_id:
        return "Invalid native AGY conversation ID"
    if proc_root is None and (reason := unsupported_platform_reason()):
        return reason
    native_home = Path(native_home) if native_home is not None else Path.home() / ".gemini/antigravity-cli"
    proc_root = Path(proc_root) if proc_root is not None else Path("/proc")
    reason = _target_lock_reason(native_id, native_home, proc_root, exclude_pid)
    if reason:
        return reason
    executable = Path(executable).expanduser()
    names = {executable.name, "agy", "agy-real"}
    try:
        processes = list(proc_root.iterdir())
    except OSError:
        return "Cannot inspect AGY processes"
    for process in processes:
        if not process.name.isdigit() or int(process.name) in {os.getpid(), exclude_pid}:
            continue
        try:
            if process.stat().st_uid != os.getuid():
                continue
            # Zombies cannot write the conversation and no longer hold presence locks.
            if (process / "stat").read_text().rsplit(") ", 1)[1].split()[0] == "Z":
                continue
            args = (process / "cmdline").read_bytes().decode(errors="replace").split("\0")
            args = [arg for arg in args if arg]
            argv_is_agy = bool(args) and (
                Path(args[0]).name in names or any(Path(arg) == executable for arg in args[:2])
            )
            try:
                binary = Path(os.readlink(process / "exe").removesuffix(" (deleted)"))
            except PermissionError:
                comm = (process / "comm").read_text().strip()
                if args and comm and comm not in names and not argv_is_agy:
                    continue
                return "Cannot confirm AGY process state (pid " + process.name + ")"
            except FileNotFoundError:
                binary = None
            if not args:
                if binary is None or binary.name in names:
                    return "Cannot confirm AGY process state (pid " + process.name + ")"
                continue
            if not (
                (binary is not None and binary.name in names)
                or argv_is_agy
            ):
                continue
            explicit = [arg.split("=", 1)[1] for arg in args if arg.startswith("--conversation=")]
            explicit += [args[i + 1] for i, arg in enumerate(args[:-1]) if arg == "--conversation"]
            if native_id in explicit:
                return "Native AGY conversation is active (pid " + process.name + ")"
            if explicit and not any(arg in {"-c", "--continue"} for arg in args):
                continue
            known = _presence_sessions(int(process.name), native_home, proc_root)
            if len(known) == 1 and native_id not in known:
                continue
            return "AGY instance has an unconfirmed conversation (pid " + process.name + ")"
        except (OSError, ValueError, IndexError):
            # Exiting processes disappear normally; unreadable live processes are not offline.
            try:
                process.stat()
            except FileNotFoundError:
                continue
            except OSError:
                pass
            return "Cannot confirm AGY process state (pid " + process.name + ")"
    return None

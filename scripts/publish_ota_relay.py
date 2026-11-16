"""Verify a GitHub release locally and publish immutable OTA assets to a Tailscale relay host over SSH.

Trust & migration invariants:
- The relay host requires zero GitHub credentials; GitHub releases are downloaded and SHA256-verified locally.
- Release version directories under ``<remote_dir>/releases/<tag>`` are immutable once published.
- Upgrade client nodes running versions older than 0.2.11 before switching their ``update.json`` to a mirror URL.
- When clients pull over plain HTTP on Tailscale, Tailscale ACLs/routing protect transport and the relay host
  is trusted as a release publisher; SHA256SUMS detects corruption and mismatched filenames.
"""

from __future__ import annotations

import argparse
import base64
import http.client
import io
import json
import os
import re
import shlex
import subprocess
import tarfile
import tempfile
from pathlib import Path

from agent_bridge.updater import (
    REPOSITORY,
    TAG,
    _download_bytes,
    build_release_manifest,
    latest_release,
    validate_release_url,
    verify_wheel,
)

_REMOTE_APPLY_SCRIPT = """
import fcntl, hashlib, json, os, re, shutil, sys, tarfile, uuid
from pathlib import Path

def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

remote_dir = Path(sys.argv[1])
tag = sys.argv[2]
wheel = sys.argv[3]
if not remote_dir.is_absolute() or not re.fullmatch(r"v\\d+\\.\\d+\\.\\d+", tag):
    raise SystemExit("Invalid remote_dir or tag")

remote_dir.mkdir(parents=True, exist_ok=True)
os.chmod(remote_dir, 0o755)
releases_dir = remote_dir / "releases"
releases_dir.mkdir(exist_ok=True)
os.chmod(releases_dir, 0o755)

lock_fd = os.open(remote_dir / ".publish.lock", os.O_CREAT | os.O_RDWR, 0o600)
stage_dir = None
latest_stage = None
try:
    fcntl.flock(lock_fd, fcntl.LOCK_EX)
    stage_dir = releases_dir / f".stage-{tag}-{uuid.uuid4().hex[:12]}"
    stage_dir.mkdir(mode=0o755)
    allowed = {"latest.json", wheel, "SHA256SUMS"}
    with tarfile.open(fileobj=sys.stdin.buffer, mode="r|*") as tf:
        seen = set()
        for member in tf:
            if not member.isreg() or member.name not in allowed or member.name in seen:
                raise SystemExit("Unexpected tar entry")
            stream = tf.extractfile(member)
            if stream is None:
                raise SystemExit("Unreadable tar entry")
            out_path = stage_dir / member.name
            with out_path.open("wb") as fh:
                shutil.copyfileobj(stream, fh)
            os.chmod(out_path, 0o644)
            seen.add(member.name)
        if seen != allowed:
            raise SystemExit("Incomplete upload")

    expected = None
    for line in (stage_dir / "SHA256SUMS").read_text().splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1] == wheel:
            expected = parts[0].lower()
            break
    digest = file_sha256(stage_dir / wheel)
    if expected != digest:
        raise SystemExit("Remote SHA256 verification failed")
    sums_digest = file_sha256(stage_dir / "SHA256SUMS")

    manifest = json.loads((stage_dir / "latest.json").read_text())
    if not isinstance(manifest, dict) or manifest.get("tag_name") != tag:
        raise SystemExit("Remote manifest mismatch")

    latest_path = remote_dir / "latest.json"
    if latest_path.exists():
        if latest_path.is_symlink() or not latest_path.is_file():
            raise SystemExit(3)
        current_manifest = json.loads(latest_path.read_text())
        current_tag = current_manifest.get("tag_name") if isinstance(current_manifest, dict) else None
        if not isinstance(current_tag, str) or not re.fullmatch(r"v\\d+\\.\\d+\\.\\d+", current_tag):
            raise SystemExit(3)
        new_ver = tuple(int(part) for part in tag[1:].split("."))
        cur_ver = tuple(int(part) for part in current_tag[1:].split("."))
        if new_ver < cur_ver:
            raise SystemExit(3)

    latest_stage = remote_dir / f".latest.json.{uuid.uuid4().hex[:12]}"
    shutil.copyfile(stage_dir / "latest.json", latest_stage)
    os.chmod(latest_stage, 0o644)
    (stage_dir / "latest.json").unlink()

    target_dir = releases_dir / tag
    immutable_preserved = False
    if target_dir.exists():
        if target_dir.is_symlink() or not target_dir.is_dir():
            raise SystemExit(2)
        entries = {p.name for p in target_dir.iterdir()}
        existing_wheel = target_dir / wheel
        existing_sums = target_dir / "SHA256SUMS"
        if (
            entries != {wheel, "SHA256SUMS"}
            or existing_wheel.is_symlink()
            or existing_sums.is_symlink()
            or not existing_wheel.is_file()
            or not existing_sums.is_file()
            or file_sha256(existing_wheel) != digest
            or file_sha256(existing_sums) != sums_digest
        ):
            raise SystemExit(2)
        immutable_preserved = True
    else:
        os.replace(stage_dir, target_dir)

    os.replace(latest_stage, remote_dir / "latest.json")
    print(json.dumps({"published": tag, "wheel": wheel, "sha256": digest, "remote_dir": str(remote_dir), "immutable_preserved": immutable_preserved}))
finally:
    if latest_stage is not None:
        latest_stage.unlink(missing_ok=True)
    if stage_dir is not None and stage_dir.exists():
        shutil.rmtree(stage_dir, ignore_errors=True)
    os.close(lock_fd)
"""


def build_tar_payload(directory: Path, filenames: tuple[str, ...]) -> bytes:
    """Create a deterministic tar archive containing exact top-level release files."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for name in filenames:
            path = directory / name
            data = path.read_bytes()
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            info.mode = 0o644
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _resolve_release_meta(repository: str, tag: str | None) -> dict[str, object]:
    endpoint = f"repos/{repository}/releases/tags/{tag}" if tag else f"repos/{repository}/releases/latest"
    try:
        proc = subprocess.run(
            ["gh", "api", "--hostname", "github.com", endpoint],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
    except subprocess.CalledProcessError as error:
        raise RuntimeError(f"gh api failed (exit {error.returncode})") from None
    except subprocess.TimeoutExpired:
        raise RuntimeError("gh api timed out") from None
    try:
        meta = json.loads(proc.stdout)
    except json.JSONDecodeError:
        raise ValueError("Invalid GitHub release JSON") from None
    if not isinstance(meta, dict):
        raise ValueError("Invalid GitHub release metadata")
    resolved_tag = meta.get("tag_name")
    if not isinstance(resolved_tag, str) or not TAG.fullmatch(resolved_tag):
        raise ValueError("Invalid GitHub release tag")
    if meta.get("draft") or meta.get("prerelease"):
        raise ValueError("Refusing draft or prerelease release")
    return meta


def _verify_published_mirror(verify_url: str, repository: str, rel_tag: str, wheel: str, digest: str) -> None:
    checked = latest_release(repository, release_url=verify_url)
    if checked.get("tag") != rel_tag or checked.get("wheel") != wheel:
        raise ValueError("Mirror manifest mismatch")
    with tempfile.TemporaryDirectory(prefix="ota-relay-verify-") as verify_tmp:
        verify_dir = Path(verify_tmp)
        (verify_dir / "SHA256SUMS").write_bytes(
            _download_bytes(f"{verify_url}/releases/{rel_tag}/SHA256SUMS", max_bytes=65_536, timeout=30)
        )
        (verify_dir / wheel).write_bytes(
            _download_bytes(f"{verify_url}/releases/{rel_tag}/{wheel}", max_bytes=100_000_000, timeout=120)
        )
        mirrored_digest = verify_wheel(verify_dir, wheel)
    if mirrored_digest != digest:
        raise ValueError("Mirrored wheel digest mismatch")


def publish_release(
    *,
    host: str,
    remote_dir: str,
    repository: str = REPOSITORY,
    tag: str | None = None,
    verify_url: str | None = None,
) -> dict[str, object]:
    """Download a verified GitHub release locally and upload it atomically to a Tailscale relay host."""
    if not os.path.isabs(remote_dir) or not remote_dir.strip():
        raise ValueError("remote_dir must be an absolute path")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", host):
        raise ValueError("Invalid SSH host alias")
    if tag is not None and not TAG.fullmatch(tag):
        raise ValueError("Invalid release tag")
    normalized_verify = validate_release_url(verify_url) if verify_url else None

    meta = _resolve_release_meta(repository, tag)
    rel_tag = str(meta["tag_name"])
    wheel = f"cli_agent_bridge-{rel_tag[1:]}-py3-none-any.whl"
    with tempfile.TemporaryDirectory(prefix="ota-relay-stage-") as tmp:
        stage_dir = Path(tmp)
        try:
            subprocess.run(
                [
                    "gh",
                    "release",
                    "download",
                    rel_tag,
                    "--repo",
                    f"github.com/{repository}",
                    "--dir",
                    str(stage_dir),
                    "--pattern",
                    wheel,
                    "--pattern",
                    "SHA256SUMS",
                ],
                capture_output=True,
                check=True,
                timeout=120,
            )
        except subprocess.CalledProcessError as error:
            raise RuntimeError(f"gh release download failed (exit {error.returncode})") from None
        except subprocess.TimeoutExpired:
            raise RuntimeError("gh release download timed out") from None
        digest = verify_wheel(stage_dir, wheel)
        wheel_size = (stage_dir / wheel).stat().st_size
        sums_size = (stage_dir / "SHA256SUMS").stat().st_size
        rel_id = meta.get("id") if isinstance(meta.get("id"), int) else None
        manifest = build_release_manifest(rel_tag, wheel_size, sums_size, release_id=rel_id)
        (stage_dir / "latest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        tar_bytes = build_tar_payload(stage_dir, ("latest.json", wheel, "SHA256SUMS"))

    script_b64 = base64.b64encode(_REMOTE_APPLY_SCRIPT.encode("utf-8")).decode("ascii")
    runner = "import base64,sys; exec(compile(base64.b64decode(sys.argv.pop(1)),'remote_apply','exec'))"
    remote_cmd = "python3 -c " + " ".join(shlex.quote(part) for part in (runner, script_b64, remote_dir, rel_tag, wheel))
    ssh_cmd = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        host,
        remote_cmd,
    ]
    try:
        res = subprocess.run(ssh_cmd, input=tar_bytes, capture_output=True, check=True, timeout=120)
    except subprocess.CalledProcessError as error:
        if error.returncode == 2:
            raise ValueError(f"Refusing to overwrite existing immutable release {rel_tag} with different contents") from None
        if error.returncode == 3:
            raise ValueError(f"Refusing to roll back latest mirror manifest to older release {rel_tag}") from None
        raise RuntimeError(f"ssh relay publish failed (exit {error.returncode})") from None
    except subprocess.TimeoutExpired:
        raise RuntimeError("ssh relay publish timed out") from None

    try:
        uploaded = json.loads(res.stdout.decode("utf-8").strip())
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise RuntimeError("Invalid JSON response from relay publish") from None
    if not isinstance(uploaded, dict) or uploaded.get("sha256") != digest or uploaded.get("published") != rel_tag:
        raise RuntimeError("Uploaded relay metadata verification failed")

    if normalized_verify:
        try:
            _verify_published_mirror(normalized_verify, repository, rel_tag, wheel, digest)
        except (OSError, ValueError, RuntimeError, http.client.HTTPException, json.JSONDecodeError) as error:
            raise RuntimeError(
                f"SSH upload of {rel_tag} succeeded, but HTTP mirror verification failed ({type(error).__name__})"
            ) from None
        uploaded["verified_url"] = normalized_verify
    return uploaded


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="relay", help="SSH host alias for the OTA relay server")
    parser.add_argument("--remote-dir", default="/srv/ota/cli-agent-bridge", help="Remote OTA root directory")
    parser.add_argument("--repository", default=REPOSITORY, help="GitHub repository owner/name")
    parser.add_argument("--tag", help="Release tag vX.Y.Z (default: latest stable GitHub release)")
    parser.add_argument(
        "--verify-url",
        default="http://192.0.2.20:8088/cli-agent-bridge",
        help="Optional mirror base URL to verify after publishing (pass '' to skip)",
    )
    args = parser.parse_args(argv)
    try:
        result = publish_release(
            host=args.host,
            remote_dir=args.remote_dir,
            repository=args.repository,
            tag=args.tag,
            verify_url=args.verify_url or None,
        )
    except (OSError, ValueError, RuntimeError, http.client.HTTPException, json.JSONDecodeError) as error:
        message = str(error) if isinstance(error, (ValueError, RuntimeError)) else type(error).__name__
        print(json.dumps({"status": "failed", "error_type": type(error).__name__, "message": message}, ensure_ascii=False))
        raise SystemExit(1) from None
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()

from __future__ import annotations

import codecs
import hashlib
import json
import os
import stat
import uuid

from .errors import BridgeError


def brief(value, limit=4000):
    if isinstance(value, str):
        return value[:limit] + ("\n[truncated; read the artifact]" if len(value) > limit else "")
    if isinstance(value, list):
        return [brief(item, limit) for item in value[:20]]
    if isinstance(value, dict):
        return {key: brief(item, limit) for key, item in list(value.items())[:80]}
    return value


def _open_file(folder, name, flags):
    directory = os.open(folder, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        return os.open(name, flags | os.O_NOFOLLOW, 0o600, dir_fd=directory)
    finally:
        os.close(directory)


def save(state, value):
    key = str(uuid.uuid4())
    folder = state / "artifacts"
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = folder / (key + ".json")
    raw = json.dumps(value, ensure_ascii=False, indent=2).encode()
    with os.fdopen(_open_file(folder, path.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL), "wb") as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(raw)
    return {"artifact_id": key, "path": str(path), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def read(state, artifact_id, metadata, offset, limit):
    if type(offset) is not int or offset < 0 or type(limit) is not int or not 4 <= limit <= 65536:
        raise BridgeError("invalid_range", "Use a nonnegative byte offset and a limit from 4 to 65536 bytes")
    changed = "The stored artifact changed or the offset exceeds its size"
    try:
        if str(uuid.UUID(artifact_id)) != artifact_id or metadata["artifact_id"] != artifact_id:
            raise ValueError("Invalid artifact identifier")
    except (ValueError, KeyError) as error:
        raise BridgeError("artifact_changed", changed) from error
    # The recorded absolute path may refer to the original state directory after a restore.
    path = state / "artifacts" / (artifact_id + ".json")
    try:
        with os.fdopen(_open_file(path.parent, path.name, os.O_RDONLY | os.O_NONBLOCK), "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size != metadata["bytes"] or offset > info.st_size:
                raise BridgeError("artifact_changed", changed)
            stream.seek(offset)
            raw = stream.read(limit)
    except OSError as error:
        raise BridgeError("artifact_changed", changed) from error
    decoder = codecs.getincrementaldecoder("utf-8")()
    try:
        content = decoder.decode(raw, final=offset + len(raw) == metadata["bytes"])
    except UnicodeDecodeError as error:
        raise BridgeError("invalid_range", "The offset must point to a UTF-8 character boundary") from error
    next_offset = offset + len(raw) - len(decoder.getstate()[0])
    return {
        **metadata,
        "path": str(path),
        "content": content,
        "offset": offset,
        "next_offset": next_offset,
        "eof": next_offset == metadata["bytes"],
        "encoding": "utf-8",
        "offset_unit": "bytes",
    }

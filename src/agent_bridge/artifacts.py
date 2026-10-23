from __future__ import annotations

import codecs
import hashlib
import json
import uuid
from pathlib import Path

from .errors import BridgeError


def brief(value, limit=4000):
    if isinstance(value, str):
        return value[:limit] + ("\n[truncated; read the artifact]" if len(value) > limit else "")
    if isinstance(value, list):
        return [brief(item, limit) for item in value[:20]]
    if isinstance(value, dict):
        return {key: brief(item, limit) for key, item in list(value.items())[:80]}
    return value


def save(state, value):
    key = str(uuid.uuid4())
    folder = state / "artifacts"
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = folder / (key + ".json")
    raw = json.dumps(value, ensure_ascii=False, indent=2).encode()
    with path.open("xb") as stream:
        path.chmod(0o600)
        stream.write(raw)
    return {"artifact_id": key, "path": str(path), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def read(metadata, offset, limit):
    if type(offset) is not int or offset < 0 or type(limit) is not int or not 4 <= limit <= 65536:
        raise BridgeError("invalid_range", "Use a nonnegative byte offset and a limit from 4 to 65536 bytes")
    path = Path(metadata["path"])
    if path.is_symlink() or path.stat().st_size != metadata["bytes"] or offset > metadata["bytes"]:
        raise BridgeError("artifact_changed", "The stored artifact changed or the offset exceeds its size")
    with path.open("rb") as stream:
        stream.seek(offset)
        raw = stream.read(limit)
    decoder = codecs.getincrementaldecoder("utf-8")()
    try:
        content = decoder.decode(raw, final=offset + len(raw) == metadata["bytes"])
    except UnicodeDecodeError as error:
        raise BridgeError("invalid_range", "The offset must point to a UTF-8 character boundary") from error
    next_offset = offset + len(raw) - len(decoder.getstate()[0])
    return {
        **metadata,
        "content": content,
        "offset": offset,
        "next_offset": next_offset,
        "eof": next_offset == metadata["bytes"],
        "encoding": "utf-8",
        "offset_unit": "bytes",
    }

"""Private configuration, atomic records and explicit operation locks."""

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import tempfile


class FlowError(Exception):
    """An actionable, safely reportable error."""


def now():
    return datetime.now(timezone.utc).isoformat()


def identifier(value):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", value):
        raise FlowError("Names must contain 1-80 ASCII letters, digits, '_' or '-'.")
    return value


def remote_path(value):
    path = PurePosixPath(value)
    if not value.startswith("/") or value == "/" or ".." in path.parts or any(c in value for c in "\n\r\x00"):
        raise FlowError("Remote roots must be explicit absolute POSIX paths, not '/' or traversal paths.")
    return str(path)


def relative_path(value):
    if "\\" in value or any(c in value for c in "\n\r\x00"):
        raise FlowError("Use forward slashes for paths relative to source.")
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or ".." in path.parts or path.parts[0].endswith(":"):
        raise FlowError("Expected a safe path relative to the source directory.")
    return str(path)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as error:
        raise FlowError(f"Cannot read valid JSON: {path}") from error


def write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink():
        raise FlowError("Refusing to replace a symbolic-link record.")
    descriptor, temporary = tempfile.mkstemp(prefix=".cf-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(data, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@contextmanager
def lock(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as error:
        raise FlowError(f"Operation already locked: {path}. Inspect the owning process before removing a stale lock.") from error
    try:
        os.write(descriptor, str(os.getpid()).encode("ascii"))
        os.close(descriptor)
        yield
    finally:
        path.unlink(missing_ok=True)


class Store:
    def __init__(self, home=None):
        if home is None:
            if os.name == "nt":
                base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
            else:
                base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
            home = base / "clusterflow"
        self.home = Path(home).expanduser().absolute()

    def get(self, kind, name):
        if kind not in ("clusters", "projects"):
            raise FlowError("Unknown configuration category.")
        return read_json(self.home / kind / f"{identifier(name)}.json")

    def put(self, kind, name, data, replace=False):
        path = self.home / kind / f"{identifier(name)}.json"
        with lock(self.home / "config.lock"):
            if path.exists() and not replace:
                raise FlowError("Configuration already exists; explicit --replace is required.")
            write_json(path, data)

    def names(self, kind):
        return sorted(path.stem for path in (self.home / kind).glob("*.json"))


def execution_path(project, execution):
    return Path(project["record_root"]) / "executions" / identifier(execution)


def record(project, execution):
    return read_json(execution_path(project, execution) / "record.json")


def save_record(project, data, event, **details):
    data["updated"] = now()
    data.setdefault("events", []).append({"at": now(), "event": event, **details})
    write_json(execution_path(project, data["execution_id"]) / "record.json", data)

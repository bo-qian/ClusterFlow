"""Freeze actual source/input bytes, then test exactly that artifact."""

from datetime import datetime, timezone
import copy
import fnmatch
import os
from pathlib import Path
import re
import subprocess
import uuid

from .storage import FlowError, digest, execution_path, now, record, relative_path, save_record, write_json

EXCLUDED = {".git", ".venv", "venv", "__pycache__", ".pytest_cache", ".clusterflow", "outputs", "results", "checkpoints", "node_modules", "build", "dist", ".ssh"}
SENSITIVE = re.compile(r"(^\.env($|\.)|^id_(rsa|ed25519)|\.(pem|key)$|^credentials|^secrets)", re.I)
FIELD_EXTENSIONS = {".vtk", ".vtu", ".pvtu", ".vtp", ".pvd", ".bp", ".h5", ".hdf5", ".xdmf", ".e"}
MAX_FILE = 64 * 1024 * 1024
MAX_TOTAL = 512 * 1024 * 1024


def source_files(source, excludes=()):
    source = Path(source)
    files = []
    for root, directories, names in os.walk(source, followlinks=False):
        directories[:] = sorted(name for name in directories if name not in EXCLUDED and not SENSITIVE.search(name))
        for name in directories:
            if (Path(root) / name).is_symlink():
                raise FlowError("Symlink directories are not included in snapshots; use a real source tree.")
        for name in sorted(names):
            path = Path(root) / name
            rel = path.relative_to(source).as_posix()
            if SENSITIVE.search(name) or path.suffix.lower() in FIELD_EXTENSIONS or any(fnmatch.fnmatch(rel, pattern) or fnmatch.fnmatch(name, pattern) for pattern in excludes):
                continue
            if path.is_symlink() or not path.is_file():
                raise FlowError(f"Not a regular source file: {rel}")
            if path.stat().st_size > MAX_FILE:
                raise FlowError(f"Source file exceeds 64 MiB: {rel}. Exclude bulk data explicitly.")
            files.append((rel, path))
    return sorted(files)


def contents(source, excludes=()):
    result, total = {}, 0
    for rel, path in source_files(source, excludes):
        data = path.read_bytes()
        total += len(data)
        if total > MAX_TOTAL or len(data) > MAX_FILE:
            raise FlowError("Snapshot exceeds safety limits; keep bulk results outside source.")
        if re.search(rb"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----", data):
            raise FlowError(f"Possible private key in source: {rel}; remove it before preparing.")
        result[rel] = {"sha256": digest(data), "size": len(data), "executable": bool(path.stat().st_mode & 0o111)}
    return result


def prepare(project, input_path, resume=None, timeout=600, source_execution=None):
    if source_execution:
        if resume:
            raise FlowError("Choose historical reproduction or continuation explicitly, not both.")
        original = record(project, source_execution)
        verify(project, original)
        original_source = execution_path(project, source_execution) / "source"
        old_config = copy.deepcopy(original["project_config"])
        old_config.update(local_source=str(original_source), record_root=project["record_root"], local_results=project["local_results"])
        project = old_config
    project = copy.deepcopy(project)
    if timeout < 1:
        raise FlowError("Check timeout must be positive.")
    input_path = relative_path(input_path)
    source = Path(project["local_source"])
    before = contents(source, project["exclude"])
    if input_path not in before:
        raise FlowError("Input must be a regular, included file inside local_source.")
    parent = record(project, resume) if resume else None
    execution = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:12]
    directory = execution_path(project, execution)
    frozen = directory / "source"
    frozen.mkdir(parents=True, mode=0o700)
    try:
        for rel, info in before.items():
            data = (source / rel).read_bytes()
            if digest(data) != info["sha256"]:
                raise FlowError("Local source changed during preparation; prepare again.")
            target = frozen / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            target.chmod(0o700 if info["executable"] else 0o600)
        if contents(source, project["exclude"]) != before:
            raise FlowError("Local source changed during preparation; prepare again.")
        launch = {key: project[key] for key in ("command", "check_command", "environment", "resources", "exclude")}
        manifest = {"files": before, "input": input_path, "input_sha256": before[input_path]["sha256"], "launch": launch}
        import json
        manifest_hash = digest(json.dumps(manifest, sort_keys=True).encode())
        values = {"input": str(frozen / input_path), "code": str(frozen), "run": str(directory), "tasks": str(project["resources"].get("tasks", 1))}
        check = [expand(arg, values) for arg in project["check_command"]]
        try:
            result = subprocess.run(check, cwd=frozen, capture_output=True, timeout=timeout, stdin=subprocess.DEVNULL)
        except subprocess.TimeoutExpired as error:
            raise FlowError("Project checks timed out; no submission was prepared.") from error
        (directory / "check.stdout.log").write_bytes(result.stdout)
        (directory / "check.stderr.log").write_bytes(result.stderr)
        if result.returncode:
            raise FlowError(f"Project check failed (exit {result.returncode}); inspect local check logs. No submission prepared.")
        if contents(frozen, project["exclude"]) != before:
            raise FlowError("Project checks modified source/input. No verified snapshot was prepared.")
        write_json(directory / "manifest.json", manifest)
        data = {
            "project": project["name"], "cluster": project["cluster"],
            "execution_id": execution, "run_id": parent["run_id"] if parent else execution,
            "parent_execution": resume, "created": now(), "state": "PREPARED",
            "reproduces_execution": source_execution,
            "manifest_sha256": manifest_hash, "input": input_path,
            "project_config": project, "check": {"argv": check, "exit_code": 0},
            "remote_code": project["remote_source"] + "/clusterflow/" + execution + "/source",
            "remote_run": project["remote_runs"] + "/" + (parent["run_id"] if parent else execution) + "/executions/" + execution,
            "events": [],
        }
        save_record(project, data, "prepared", manifest_sha256=manifest_hash)
        return data
    except Exception:
        write_json(directory / "PREPARE_FAILED.json", {"at": now(), "reason": "Preparation or checks failed; no run is submit-ready."})
        raise


def expand(value, mapping):
    # Replace only our named tokens; braces in application arguments remain untouched.
    for key, replacement in mapping.items():
        value = value.replace("{" + key + "}", replacement)
    return value


def verify(project, data):
    from .storage import read_json
    import json
    directory = execution_path(project, data["execution_id"])
    manifest = read_json(directory / "manifest.json")
    if digest(json.dumps(manifest, sort_keys=True).encode()) != data["manifest_sha256"]:
        raise FlowError("Snapshot manifest was modified after checking.")
    if manifest["launch"] != {key: data["project_config"][key] for key in manifest["launch"]}:
        raise FlowError("Checked launch settings were modified; prepare a new execution.")
    if contents(directory / "source", data["project_config"]["exclude"]) != manifest["files"]:
        raise FlowError("Snapshot content changed after checks; prepare a new execution.")
    return manifest

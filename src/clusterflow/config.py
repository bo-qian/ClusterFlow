"""Reviewable per-user profiles, without embedded passwords."""

from pathlib import Path
import re

from .storage import FlowError, identifier, remote_path


def cluster_profile(name, host, user, port=22, auth="agent", key_file=None, jump=None):
    identifier(name)
    if not host or any(c.isspace() for c in host) or any(c in host for c in "/\\\x00"):
        raise FlowError("Invalid host name or address.")
    if not re.fullmatch(r"[A-Za-z0-9_.@-]+", user or "") or not 1 <= int(port) <= 65535:
        raise FlowError("Invalid SSH username or port.")
    if auth not in ("password", "agent", "key"):
        raise FlowError("Authentication must be password, agent or key.")
    result = {"name": name, "host": host, "user": user, "port": int(port), "auth": auth, "jump": jump}
    if jump:
        identifier(jump)
        if jump == name:
            raise FlowError("A cluster cannot be its own jump host.")
    if auth == "key":
        key = Path(key_file or "").expanduser()
        if not key_file or not key.is_file():
            raise FlowError("Choose an existing SSH key explicitly, or use an SSH agent.")
        result["key_file"] = str(key.absolute())
    return result


def argv(value, label):
    if not isinstance(value, list) or not value or any(not isinstance(x, str) or not x or "\x00" in x for x in value):
        raise FlowError(f"{label} must be a nonempty JSON array of argument strings.")
    return value


def validate_project(data):
    result = dict(data)
    identifier(result.get("name", ""))
    identifier(result.get("cluster", ""))
    source = Path(result.get("local_source", "")).expanduser().resolve()
    if not result.get("local_source") or not source.is_dir() or source == Path(source.anchor):
        raise FlowError("local_source must be an existing, specific project directory.")
    result["local_source"] = str(source)
    for field in ("remote_source", "remote_runs"):
        result[field] = remote_path(result.get(field, ""))
    if result["remote_source"] == result["remote_runs"]:
        raise FlowError("Separate remote_source and remote_runs directories.")
    if not result.get("local_results"):
        raise FlowError("Set a separate local_results directory.")
    results = Path(result["local_results"]).expanduser().resolve()
    if results == Path(results.anchor) or results == source or source in results.parents:
        raise FlowError("local_results must be outside local_source, not a filesystem root.")
    result["local_results"] = str(results)
    records = Path(result.get("record_root", results / ".clusterflow")).expanduser().resolve()
    if records == Path(records.anchor) or records == source or source in records.parents:
        raise FlowError("record_root must be outside source and cannot be a filesystem root.")
    result["record_root"] = str(records)
    mode = result.get("code_mode", "snapshot")
    if mode not in ("snapshot", "remote"):
        raise FlowError("code_mode must be snapshot or remote.")
    if mode == "remote":
        result["remote_check_command"] = argv(result.get("remote_check_command"), "remote_check_command")
    result["command"] = argv(result.get("command"), "command")
    result["check_command"] = argv(result.get("check_command"), "check_command")
    resources = result.setdefault("resources", {})
    if not isinstance(resources, dict) or set(resources) - {"partition", "account", "tasks", "nodes", "memory_mb", "nodelist", "time"}:
        raise FlowError("Unsupported Slurm resource key.")
    for key in ("tasks", "nodes", "memory_mb"):
        if key in resources and (type(resources[key]) is not int or resources[key] < 1):
            raise FlowError(f"resources.{key} must be a positive integer.")
    for key in ("partition", "account", "nodelist", "time"):
        if key in resources and not re.fullmatch(r"[A-Za-z0-9_.,:+\[\]-]+", str(resources[key])):
            raise FlowError(f"Invalid resources.{key}.")
    env = result.setdefault("environment", {})
    if not isinstance(env, dict) or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) or not isinstance(value, str) or "\x00" in value for key, value in env.items()):
        raise FlowError("environment must map valid variable names to non-secret strings.")
    if any(re.search(r"PASSWORD|TOKEN|SECRET|PRIVATE_KEY", key, re.I) for key in env):
        raise FlowError("Do not put credentials in the project environment.")
    if any(key.startswith("CLUSTERFLOW_") or key == "PYTHONDONTWRITEBYTECODE" for key in env):
        raise FlowError("ClusterFlow runtime variables are reserved and cannot be overridden.")
    result.setdefault("exclude", [])
    if not isinstance(result["exclude"], list) or any(not isinstance(value, str) for value in result["exclude"]):
        raise FlowError("exclude must be an array of glob patterns.")
    allowed = {"code_mode", "remote_check_command", "name", "cluster", "local_source", "remote_source", "remote_runs", "local_results", "record_root", "command", "check_command", "resources", "environment", "exclude"}
    if set(result) - allowed:
        raise FlowError("Unknown project field(s); do not embed passwords or private settings in examples.")
    return result

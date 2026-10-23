"""Snapshot transfer, guarded Slurm operations and verifiable result retrieval."""

import csv
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import stat
import uuid

from .snapshot import expand, verify
from .storage import FlowError, digest, execution_path, lock, record, relative_path, save_record


def checked(remote, arguments):
    code, output, _ = remote.exec(arguments)
    if code:
        raise FlowError(f"Remote {arguments[0]} failed (exit {code}); no success assumed.")
    return output


def resources(remote):
    output = checked(remote, ["sinfo", "-N", "-h", "-o", "%N|%P|%t|%c|%m"])
    return [dict(zip(("node", "partition", "state", "cpus", "memory_mb"), row.split("|"))) for row in output.splitlines() if row.strip()]


def inspect_resources(remote, config):
    rows = resources(remote)
    partition = config.get("partition")
    nodes = config.get("nodelist")
    candidates = [row for row in rows if (not partition or row.get("partition", "").rstrip("*") == partition)]
    if nodes:
        if any(c in nodes for c in "[],"):
            raise FlowError("First beta accepts a single literal nodelist node; leave it unset for scheduler selection.")
        candidates = [row for row in candidates if row.get("node") == nodes]
    healthy = [row for row in candidates if not any(bad in row.get("state", "").lower() for bad in ("down", "drain", "fail", "maint", "resv", "unk"))]
    tasks = config.get("tasks", 1)
    memory = config.get("memory_mb", 0)
    healthy = [row for row in healthy if int(row.get("cpus", "0")) * config.get("nodes", 1) >= tasks and int(row.get("memory_mb", "0")) >= memory]
    if not healthy:
        raise FlowError("No healthy node matches configured partition/node/CPU/memory capacity. Inspect resources before submitting.")
    return healthy


def preview(project, data):
    manifest = verify(project, data)
    return {"execution_id": data["execution_id"], "remote_code": data["remote_code"], "remote_run": data["remote_run"], "files": list(manifest["files"]), "bytes": sum(info["size"] for info in manifest["files"].values()), "deletions": [], "shared_tree_overwrites": False}


def sync(project, data, remote):
    manifest = verify(project, data)
    root = str(PurePosixPath(data["remote_code"]).parent)
    owner = (data["manifest_sha256"] + "\n").encode()
    marker = root + "/SNAPSHOT_SHA256"
    if remote.exists(root):
        if not remote.exists(marker) or remote.read(marker) != owner:
            raise FlowError("Remote artifact already exists without matching ownership; refusing to overwrite.")
    else:
        remote.mkdirs(root)
        remote.write(marker, owner)
    source = execution_path(project, data["execution_id"]) / "source"
    for rel, info in manifest["files"].items():
        target = data["remote_code"] + "/" + rel
        if remote.exists(target):
            if digest(remote.read(target)) != info["sha256"]:
                raise FlowError(f"Remote snapshot conflict: {rel}; no file was overwritten.")
        else:
            remote.write(target, (source / rel).read_bytes(), mode=0o555 if info["executable"] else 0o444)
    manifest_bytes = json.dumps(manifest, sort_keys=True).encode()
    if not remote.exists(root + "/manifest.json"):
        remote.write(root + "/manifest.json", manifest_bytes, mode=0o444)
    elif remote.read(root + "/manifest.json") != manifest_bytes:
        raise FlowError("Remote manifest conflicts with the local checked artifact.")
    remote.seal(data["remote_code"])
    if not remote.exists(root + "/READY"):
        remote.write(root + "/READY", owner, mode=0o444)
    data["state"] = "SYNCED"
    save_record(project, data, "synced")
    return data


def batch_script(data):
    project = data["project_config"]
    mapping = {"code": data["remote_code"], "run": data["remote_run"], "input": data["remote_code"] + "/" + data["input"], "tasks": str(project["resources"].get("tasks", 1))}
    env = {"PYTHONDONTWRITEBYTECODE": "1", "CLUSTERFLOW_RUN_DIR": mapping["run"], "CLUSTERFLOW_INPUT": mapping["input"], "CLUSTERFLOW_RUN_ID": data["run_id"], "CLUSTERFLOW_EXECUTION_ID": data["execution_id"], **project["environment"]}
    lines = ["#!/bin/bash", "set -euo pipefail", "umask 077", "printf 'ClusterFlow job=%s host=%s\\n' \"${SLURM_JOB_ID:-unknown}\" \"$(hostname)\"", "date -u"]
    lines += ["export " + name + "=" + shlex.quote(expand(value, mapping)) for name, value in env.items()]
    lines += ["cd " + shlex.quote(mapping["code"]), "exec " + shlex.join([expand(value, mapping) for value in project["command"]])]
    return ("\n".join(lines) + "\n").encode()


def job_name(data):
    return "cf_" + data["execution_id"]


def job_status(remote, job):
    if not re.fullmatch(r"\d+", str(job)):
        raise FlowError("Invalid job ID.")
    code, output, _ = remote.exec(["squeue", "-h", "-j", str(job), "-o", "%i|%T|%R|%M|%l"])
    if code == 0 and output.strip():
        rows = [line.split("|") for line in output.splitlines() if line.strip()]
        return {"job_id": str(job), "source": "squeue", "rows": rows, "state": rows[0][1]}
    code, output, _ = remote.exec(["sacct", "-X", "-n", "-P", "-j", str(job), "--format=JobIDRaw,State,ExitCode,Elapsed,Timelimit"])
    if code:
        return {"job_id": str(job), "state": "UNKNOWN", "source": "accounting_unavailable", "rows": []}
    rows = [line.split("|") for line in output.splitlines() if line.strip() and line.split("|")[0] == str(job)]
    return {"job_id": str(job), "source": "sacct", "rows": rows, "state": rows[0][1] if rows else "UNKNOWN"}


def release(project, data, remote):
    job = data.get("job_id")
    if not job:
        raise FlowError("No confirmed job ID; reconcile first.")
    # Failure leaves a known held job; callers must not submit it again.
    details = checked(remote, ["scontrol", "show", "job", "-o", job])
    if f"JobName={job_name(data)}" not in details:
        raise FlowError("Job identity does not match this execution; refusing to release.")
    if not re.search(r"JobState=PENDING\b", details) or "JobHeldUser" not in details:
        raise FlowError("Only this execution's user-held pending job can be released.")
    if data["project_config"]["resources"].get("time") in ("0", 0) and "TimeLimit=UNLIMITED" not in details:
        raise FlowError("Requested unlimited time was not effective. Job remains held for inspection.")
    data["scheduler_details"] = details.strip()
    checked(remote, ["scontrol", "release", job])
    data["state"] = "SUBMITTED"
    save_record(project, data, "released", job_id=job)
    return data


def submit(project, data, remote):
    if data.get("job_id") or data["state"] in ("SUBMITTING", "SUBMIT_UNKNOWN", "HELD", "SUBMITTED"):
        raise FlowError("This execution was already attempted. Inspect/reconcile/release it; never blindly resubmit.")
    if data["state"] != "SYNCED":
        raise FlowError("Synchronize the verified snapshot before submitting.")
    # Revalidate every remote source byte; READY alone does not prove immutability.
    manifest = verify(project, data)
    for rel, info in manifest["files"].items():
        if digest(remote.read(data["remote_code"] + "/" + rel)) != info["sha256"]:
            raise FlowError("Remote source changed since sync; submission stopped.")
    if data.get("parent_execution"):
        parent = record(project, data["parent_execution"])
        if not parent.get("job_id") or job_status(remote, parent["job_id"])["state"].split()[0] not in ("COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "PREEMPTED"):
            raise FlowError("Parent must be a confirmed terminal job before continuation.")
    config = data["project_config"]["resources"]
    data["resource_check"] = inspect_resources(remote, config)
    remote.mkdirs(data["remote_run"] + "/logs")
    script_path = data["remote_run"] + "/submit.sh"
    script = batch_script(data)
    if not remote.exists(script_path):
        remote.write(script_path, script, mode=0o500)
    elif remote.read(script_path) != script:
        raise FlowError("Remote launch script conflicts with this execution.")
    arguments = ["sbatch", "--hold", "--parsable", "--job-name=" + job_name(data), "--comment=clusterflow:" + data["execution_id"], "--chdir=" + data["remote_code"], "--output=" + data["remote_run"] + "/logs/slurm-%j.out", "--error=" + data["remote_run"] + "/logs/slurm-%j.err"]
    options = {"partition": "partition", "account": "account", "tasks": "ntasks", "nodes": "nodes", "memory_mb": "mem", "nodelist": "nodelist", "time": "time"}
    for key, option in options.items():
        if key in config:
            value = str(config[key]) + ("M" if key == "memory_mb" else "")
            arguments.append("--" + option + "=" + value)
    arguments.append(script_path)
    data["state"] = "SUBMITTING"
    save_record(project, data, "submission_started")
    try:
        code, output, _ = remote.exec(arguments)
        if code:
            data["state"] = "SUBMIT_REJECTED"
            save_record(project, data, "submission_rejected", exit_code=code)
            raise FlowError("sbatch rejected the request. Inspect the configuration and prepare a new execution.")
        job = output.strip().split(";")[0]
        if not re.fullmatch(r"\d+", job):
            raise FlowError("No unambiguous job ID was returned.")
        data["job_id"] = job
        data["state"] = "HELD"
        save_record(project, data, "job_accepted", job_id=job)
    except Exception:
        if data["state"] != "SUBMIT_REJECTED" and not data.get("job_id"):
            data["state"] = "SUBMIT_UNKNOWN"
            save_record(project, data, "submission_response_unknown")
        raise
    return release(project, data, remote)


def reconcile(project, data, remote):
    if data.get("job_id"):
        return {"record": data, "status": job_status(remote, data["job_id"])}
    if data["state"] not in ("SUBMITTING", "SUBMIT_UNKNOWN"):
        raise FlowError("This execution has no ambiguous submission to reconcile.")
    name = job_name(data)
    queued = checked(remote, ["squeue", "-h", "--name", name, "-o", "%i|%j"])
    code, history, _ = remote.exec(["sacct", "-X", "-n", "-P", "--starttime", data["created"][:10], "--name", name, "--format=JobIDRaw,JobName%100"])
    matches = set()
    for line in (queued + "\n" + (history if code == 0 else "")).splitlines():
        parts = line.strip().split("|")
        if len(parts) >= 2 and parts[1] == name and re.fullmatch(r"\d+", parts[0]):
            matches.add(parts[0])
    if len(matches) != 1:
        raise FlowError("Submission remains ambiguous (zero or multiple matches). Do not resubmit; inspect the scheduler with the administrator.")
    data["job_id"] = matches.pop()
    data["state"] = "HELD_OR_OBSERVED"
    save_record(project, data, "reconciled", job_id=data["job_id"])
    return {"record": data, "status": job_status(remote, data["job_id"])}


def cancel(project, data, remote):
    if not data.get("job_id"):
        raise FlowError("No confirmed job ID to cancel.")
    details = checked(remote, ["scontrol", "show", "job", "-o", data["job_id"]])
    if f"JobName={job_name(data)}" not in details:
        raise FlowError("Job identity changed; cancellation refused.")
    checked(remote, ["scancel", data["job_id"]])
    save_record(project, data, "cancellation_requested", job_id=data["job_id"])
    return {"requested": True, "job_id": data["job_id"], "note": "Cancellation requested, not yet confirmed."}


def download(project, data, remote, groups=None, max_bytes=64 * 1024 * 1024, remote_dir=None):
    base = remote_dir or data["remote_run"]
    requested = groups or ["inputs", "input", "data", "logs", "log"]
    directory = Path(project["local_results"]) / data["run_id"] / data["execution_id"]
    transfers = []
    used = 0

    def collect(path, rel):
        nonlocal used
        for item in remote.list(path):
            name = item.filename
            if name in (".", "..") or "/" in name or "\\" in name or any(c in name for c in "\n\r\x00:"):
                raise FlowError("Unsafe remote filename in result listing.")
            child, relative = path + "/" + name, rel + "/" + name
            if stat.S_ISLNK(item.st_mode):
                raise FlowError("Result symlinks are not followed.")
            if stat.S_ISDIR(item.st_mode):
                collect(child, relative)
            elif stat.S_ISREG(item.st_mode):
                used += item.st_size
                if used > max_bytes:
                    raise FlowError("Download limit exceeded; request explicit groups and a larger --max-mb if needed.")
                payload = remote.read(child, max_bytes)
                if len(payload) != item.st_size:
                    raise FlowError("Remote result changed during transfer; retry after the writer settles.")
                destination = directory / relative
                if any(parent.is_symlink() for parent in (destination, *destination.parents)):
                    raise FlowError("Local download path contains a symlink.")
                destination.parent.mkdir(parents=True, exist_ok=True)
                temporary = destination.with_name(destination.name + ".part-" + uuid.uuid4().hex)
                with temporary.open("xb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, destination)
                transfers.append({"path": relative, "bytes": len(payload), "sha256": digest(payload)})

    for group in requested:
        group = relative_path(group)
        if remote.exists(base + "/" + group):
            collect(base + "/" + group, group)
    save_record(project, data, "downloaded", remote_directory=base, files=transfers)
    return {"directory": str(directory), "files": transfers, "bytes": used}


def progress(project, data):
    directory = Path(project["local_results"]) / data["run_id"] / data["execution_id"]
    result = []
    for path in sorted(directory.rglob("*.csv")) if directory.exists() else []:
        if path.stat().st_size > 64 * 1024 * 1024:
            continue
        with path.open(encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            last = None
            for row in reader:
                if None in row or any(value is None for value in row.values()):
                    continue
                try:
                    if "Step" in row:
                        float(row["Step"])
                except (ValueError, TypeError):
                    continue
                last = row
            if last:
                result.append({"path": str(path.relative_to(directory)), "latest_complete_row": last})
    return result

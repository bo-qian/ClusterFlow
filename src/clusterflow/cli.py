"""Explicit actions; status/doctor remain offline and credential-free."""

import argparse
import getpass
import importlib.util
import json
from pathlib import Path
import sys
import warnings

from . import __version__
from .storage import FlowError, Store, execution_path, lock, read_json, record, save_record

COMMANDS = ["status", "doctor", "install-skill", "init", "auth", "project", "prepare", "sync", "submit", "nodes", "query", "reconcile", "release", "cancel", "download", "progress", "runs"]


def capability_report():
    return {"name": "ClusterFlow", "version": __version__, "stage": "beta", "offline_only": False,
            "implemented": COMMANDS, "planned": [], "remote_verified": False,
            "verification": "Automated fake-transport/Slurm and isolated install tests; live-cluster acceptance still required.",
            "limitations": ["Slurm only", "VPN established externally", "passwords require an available system keyring",
                           "encrypted private keys should use an SSH agent", "application output/checkpoint paths must be explicit",
                           "scientific equivalence requires project-owned checks"]}


def diagnostic_report():
    checks = [{"name": "python", "ok": sys.version_info >= (3, 11),
               "actual": ".".join(str(part) for part in sys.version_info[:3]), "required": ">=3.11"}]
    checks += [{"name": name, "ok": importlib.util.find_spec(name) is not None} for name in ("paramiko", "keyring")]
    return {"name": "ClusterFlow", "version": __version__, "offline_only": True,
            "ok": all(item["ok"] for item in checks), "checks": checks,
            "not_checked": ["credentials", "ssh", "remote_environment", "scheduler", "project_configuration", "numerical_regression"]}


def hidden_password():
    if not sys.stdin.isatty():
        raise FlowError("Passwords require an interactive local terminal, never chat, arguments, pipes or environment variables.")
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        try:
            return getpass.getpass("Cluster password (system credential store): ")
        except getpass.GetPassWarning as error:
            raise FlowError("This terminal cannot hide input. Use a secure local terminal; echoed password fallback is forbidden.") from error


def setup(store, args):
    from .config import cluster_profile
    from .transport import Remote, credential_backend, fingerprint, password_set, probe_key, trust_key

    def required(value, prompt):
        if value:
            return value
        if not sys.stdin.isatty():
            raise FlowError("Missing setup information; supply host/user flags or use an interactive local terminal.")
        return input(prompt).strip()

    name = required(args.name, "Cluster alias: ")
    if name in store.names("clusters") and not args.replace:
        raise FlowError("Cluster already exists; use --replace deliberately or 'auth set' for password rotation.")
    profile = cluster_profile(name, required(args.host, "Host: "), required(args.user, "Username: "),
                              args.port, args.auth, args.key_file, args.jump)
    if args.auth == "password":
        credential_backend()
    key = probe_key(store, profile)
    expected = args.fingerprint
    if not expected:
        print("Observed fingerprint: " + fingerprint(key), file=sys.stderr)
        print("Verify it with your administrator/trusted source, then enter the full fingerprint.", file=sys.stderr)
        expected = required(None, "Verified fingerprint: ")
    with lock(store.home / "hostkeys.lock"):
        trust_key(store, profile, key, expected)
    password = hidden_password() if args.auth == "password" else None
    with Remote(store, profile, password=password) as remote:
        if remote.exec(["true"])[0]:
            raise FlowError("Read-only connection verification failed.")
    if password is not None:
        password_set(profile, password)
    store.put("clusters", name, profile, replace=args.replace)
    return {"cluster": name, "connection_verified": True, "config_directory": str(store.home),
            "credential_saved": args.auth == "password", "project_bound": False}


def require_yes(args):
    if not args.yes:
        raise FlowError("Remote mutation requires --yes for the user's requested operation.")


def dispatch(args):
    if args.command == "status":
        return capability_report()
    if args.command == "doctor":
        return diagnostic_report()
    if args.command == "install-skill":
        from .installer import install
        return install(Path(__file__).resolve().parents[2], args.target, args.update)
    store = Store(args.home)
    if args.command == "init":
        return setup(store, args)
    if args.command == "auth":
        from .transport import Remote, credential_backend, credential_id, password_set
        profile = store.get("clusters", args.cluster)
        if profile["auth"] != "password":
            raise FlowError("This profile does not use password authentication.")
        if args.action == "forget":
            require_yes(args)
            credential_backend().delete_password("clusterflow", credential_id(profile))
            return {"credential_removed": True, "cluster": args.cluster}
        credential_backend()
        password = hidden_password()
        with Remote(store, profile, password=password) as remote:
            if remote.exec(["true"])[0]:
                raise FlowError("Credential verification failed.")
        password_set(profile, password)
        return {"credential_updated": True, "cluster": args.cluster}
    if args.command == "project":
        if args.action == "list":
            return {"projects": store.names("projects"), "clusters": store.names("clusters")}
        if args.action == "show":
            return store.get("projects", args.name)
        from .config import validate_project
        if not args.trust_hooks:
            raise FlowError("Review launch/check commands, then use --trust-hooks to bind executable hooks.")
        project = validate_project(read_json(args.file))
        store.get("clusters", project["cluster"])
        store.put("projects", project["name"], project, replace=args.replace)
        return {"project_bound": True, "project": project["name"], "config_directory": str(store.home)}
    if args.command == "nodes":
        from .transport import Remote
        from .jobs import resources
        with Remote(store, store.get("clusters", args.cluster)) as remote:
            return {"nodes": resources(remote)}
    if args.command == "query" and args.job:
        if not args.cluster:
            raise FlowError("A raw --job query also requires --cluster.")
        from .transport import Remote
        from .jobs import job_status
        with Remote(store, store.get("clusters", args.cluster)) as remote:
            return job_status(remote, args.job)
    project = store.get("projects", args.project)
    if args.command == "prepare":
        from .snapshot import prepare
        if args.resume and not args.confirm_resume:
            raise FlowError("Review checkpoint/physical compatibility, then provide --confirm-resume.")
        return prepare(project, args.input, args.resume, args.check_timeout, args.from_execution, args.label)
    if args.command == "runs":
        return {"executions": [read_json(path) for path in sorted((Path(project["record_root"]) / "executions").glob("*/record.json"))]}
    from . import jobs
    if args.command == "progress":
        return {"execution_id": args.execution, "csv": jobs.progress(project, record(project, args.execution))}
    with lock(execution_path(project, args.execution) / "operation.lock"):
        data = record(project, args.execution)
        if args.command == "sync" and not args.yes:
            return jobs.preview(project, data)
        if args.command in ("submit", "cancel", "release"):
            require_yes(args)
        from .transport import Remote
        with Remote(store, store.get("clusters", data["cluster"])) as remote:
            if args.command == "sync":
                if data["state"] not in ("PREPARED", "SYNCED"):
                    raise FlowError("Cannot synchronize an already attempted execution.")
                return jobs.sync(project, data, remote)
            if args.command == "submit":
                return jobs.submit(project, data, remote)
            if args.command == "query":
                if not data.get("job_id"):
                    raise FlowError("No confirmed job ID; reconcile ambiguous submissions first.")
                result = jobs.job_status(remote, data["job_id"])
                save_record(project, data, "observed", scheduler=result)
                return result
            if args.command == "reconcile":
                return jobs.reconcile(project, data, remote)
            if args.command == "release":
                return jobs.release(project, data, remote)
            if args.command == "cancel":
                return jobs.cancel(project, data, remote)
            if args.command == "download":
                if args.remote_dir:
                    from .storage import remote_path
                    remote_path(args.remote_dir)
                if args.max_mb < 1:
                    raise FlowError("--max-mb must be positive.")
                return jobs.download(project, data, remote, args.groups, args.max_mb * 1024 * 1024, args.remote_dir)
    raise FlowError("Unsupported action.")


def make_parser():
    parser = argparse.ArgumentParser(prog="clusterflow", description="Local-first Slurm workflows; live-cluster acceptance required.")
    parser.add_argument("--version", action="version", version=f"ClusterFlow {__version__}")
    parser.add_argument("--home", help="Private configuration directory; defaults to per-OS user config.")
    commands = parser.add_subparsers(dest="command", required=True)

    def command(name, **kwargs):
        item = commands.add_parser(name, **kwargs)
        item.add_argument("--json", action="store_true")
        return item

    command("status", help="Offline capabilities; no config or credentials read.")
    command("doctor", help="Offline runtime/dependency checks.")
    item = command("install-skill", help="Deploy full skill files to an explicitly reviewed AI target.")
    item.add_argument("--target", required=True)
    item.add_argument("--update", action="store_true")
    item = command("init", help="Secure interactive setup; never accepts password arguments.")
    item.add_argument("name", nargs="?")
    for name in ("host", "user", "key-file", "jump", "fingerprint"):
        item.add_argument("--" + name)
    item.add_argument("--port", type=int, default=22)
    item.add_argument("--auth", choices=("password", "agent", "key"), default="password")
    item.add_argument("--replace", action="store_true")
    item = command("auth")
    item.add_argument("action", choices=("set", "forget"))
    item.add_argument("cluster")
    item.add_argument("--yes", action="store_true")
    item = command("project", help="Bind reviewed JSON hooks/mappings, or list/show.")
    item.add_argument("action", choices=("add", "show", "list"))
    item.add_argument("name", nargs="?")
    item.add_argument("--file")
    item.add_argument("--trust-hooks", action="store_true")
    item.add_argument("--replace", action="store_true")
    item = command("prepare", help="Prepare input-only remote runs or checked source snapshots.")
    item.add_argument("project")
    item.add_argument("--input", required=True)
    item.add_argument("--label", help="Readable job label, e.g. MODEL_G1_240x160_dt0p01; does not change application output IDs.")
    item.add_argument("--resume", help="Parent execution; checkpoint settings remain application-owned.")
    item.add_argument("--from-execution", help="Reproduce a verified historical snapshot as a new run.")
    item.add_argument("--confirm-resume", action="store_true")
    item.add_argument("--check-timeout", type=int, default=600)
    item = command("nodes")
    item.add_argument("cluster")
    item = command("runs")
    item.add_argument("project")
    for name in ("sync", "submit", "query", "reconcile", "release", "cancel", "download", "progress"):
        item = command(name)
        if name == "query":
            item.add_argument("project", nargs="?")
            item.add_argument("execution", nargs="?")
            item.add_argument("--cluster")
            item.add_argument("--job")
        else:
            item.add_argument("project")
            item.add_argument("execution")
        if name in ("sync", "submit", "release", "cancel"):
            item.add_argument("--yes", action="store_true")
        if name == "download":
            item.add_argument("--groups", nargs="+")
            item.add_argument("--remote-dir", help="Explicit directory if application outputs are outside CLUSTERFLOW_RUN_DIR.")
            item.add_argument("--max-mb", type=int, default=64)
    return parser


def main(argv=None):
    args = make_parser().parse_args(argv)
    try:
        report = dispatch(args)
        print(json.dumps(report, indent=2, ensure_ascii=True))
        return 0 if report.get("ok", True) else 1
    except FlowError as error:
        print(json.dumps({"ok": False, "error": str(error)}, ensure_ascii=True), file=sys.stderr)
        return 1
    except (OSError, ValueError, TypeError, KeyError, ImportError) as error:
        print(json.dumps({"ok": False, "error": f"{type(error).__name__}: check configuration/dependencies; no success assumed."}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

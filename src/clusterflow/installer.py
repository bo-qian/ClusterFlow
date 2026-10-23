"""Deploy the full reviewed skill; never silently overwrite an edited install."""

from datetime import datetime, timezone
from pathlib import Path
import shutil
import tempfile

from .storage import FlowError, digest, read_json, write_json

DEPLOY = ("SKILL.md", "README.md", "AGENTS.md", "LICENSE", "pyproject.toml", "src", "scripts", "references", "examples", "agents")


def inventory(root):
    return {path.relative_to(root).as_posix(): digest(path.read_bytes()) for path in sorted(Path(root).rglob("*")) if path.is_file() and "__pycache__" not in path.parts and path.name != ".clusterflow-install.json"}


def install(source, target, update=False):
    source, target = Path(source).resolve(), Path(target).expanduser().absolute()
    if not (source / "SKILL.md").is_file() or not (source / "src" / "clusterflow" / "cli.py").is_file():
        raise FlowError("Source checkout is missing the skill or required implementation.")
    if target == Path(target.anchor) or target == source or source in target.parents or target in source.parents or target.is_symlink():
        raise FlowError("Choose a separate specific target directory, not a source ancestor, symlink or filesystem root.")
    if target.exists():
        if not update:
            raise FlowError("Target exists. Use --update only for an unmodified managed installation.")
        marker = read_json(target / ".clusterflow-install.json")
        if inventory(target) != marker["files"]:
            raise FlowError("Installed files were modified or extra files exist; preserve/review them before updating.")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".clusterflow-install-", dir=target.parent))
    backup = None
    try:
        for name in DEPLOY:
            path = source / name
            if path.is_symlink():
                raise FlowError("Deployment source contains a symlink.")
            if path.is_dir():
                if any(child.is_symlink() for child in path.rglob("*")):
                    raise FlowError("Deployment source contains a symlink.")
                shutil.copytree(path, staging / name, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
            elif path.is_file():
                shutil.copy2(path, staging / name)
        write_json(staging / ".clusterflow-install.json", {"files": inventory(staging), "source": str(source)})
        if target.exists():
            backup = target.with_name(target.name + ".backup-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f"))
            target.rename(backup)
        try:
            staging.rename(target)
        except Exception:
            if backup:
                backup.rename(target)
            raise
        return {"installed": str(target), "backup": str(backup) if backup else None, "registered": False, "note": "Complete files deployed. Verify AI harness discovery or use explicit SKILL.md. Install Python dependencies in the chosen environment."}
    finally:
        if staging.exists():
            shutil.rmtree(staging)

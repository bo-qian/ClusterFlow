"""Readable scheduler labels, separate from immutable execution identities."""

from pathlib import PurePosixPath
import re

from .storage import FlowError


def readable_label(project, input_path, label=None):
    if label is not None:
        if not isinstance(label, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", label):
            raise FlowError("--label must be 1-64 ASCII letters, digits, dots, underscores or hyphens, starting with a letter or digit.")
        return label
    stem = PurePosixPath(input_path).stem
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", project["name"] + "_" + stem)
    return value[:64].rstrip("_.-")


def scheduler_name(label, execution):
    return label + "_" + execution.rsplit("_", 1)[-1]


def recorded_job_name(data):
    # Existing jobs must keep their exact historical scheduler identity.
    if "job_name" not in data:
        return "cf_" + data["execution_id"]
    value = data["job_name"]
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}", value):
        raise FlowError("Invalid recorded scheduler job name; inspect the execution record.")
    return value

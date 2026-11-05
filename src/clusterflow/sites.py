"""Bundled site observations, separate from user credentials and live state."""

import json
from importlib.resources import files

from .storage import FlowError


def site_profile(name="group-cluster"):
    if name != "group-cluster":
        raise FlowError("Unknown site. Use 'site list' to inspect bundled sites.")
    return json.loads(files("clusterflow").joinpath("data/group-cluster.json").read_text(encoding="utf-8"))


def site_list():
    profile = site_profile()
    return {"offline_only": True, "sites": [{key: profile[key] for key in ("id", "name", "last_verified")}]}

"""Bundled group facts stay offline, packaged and separate from personal auth."""

import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from clusterflow.cli import main, make_parser, setup
from clusterflow.installer import install
from clusterflow.sites import site_profile
from clusterflow.storage import FlowError, Store


class GroupSiteTests(unittest.TestCase):
    def test_site_queries_never_read_config_credentials_or_network(self):
        with patch("clusterflow.cli.Store", side_effect=AssertionError("no private config")), \
             patch("socket.socket", side_effect=AssertionError("no network")), \
             patch("clusterflow.transport.credential_backend", side_effect=AssertionError("no credentials")):
            for args in (["site", "list"], ["site", "show", "group-cluster"]):
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    self.assertEqual(main(args), 0)
                self.assertTrue(json.loads(out.getvalue())["offline_only"])

    def test_unknown_site_is_not_a_filesystem_path(self):
        for name in ("../secrets", "/etc/passwd", "another-site"):
            with self.subTest(name=name), self.assertRaises(FlowError):
                site_profile(name)

    def test_group_capacities_and_limits_are_explicit_observations(self):
        site = site_profile()
        self.assertFalse(site["live_state"])
        self.assertFalse(site["scheduler"]["memory_reservation_enabled"])
        self.assertFalse(site["gpu"]["fenicsx_gpu_environment_verified"])
        self.assertEqual(sum(node["cpus"] for node in site["nodes"]), 488)
        self.assertEqual(site["connection"]["username"], "ask_each_user")
        gpu_nodes = [n for n in site["nodes"] if n["gpus"]]
        self.assertEqual([(n["name"], n["gpus"][0]["count"]) for n in gpu_nodes], [("node5", 4)])

    def test_full_skill_deployment_includes_site_and_monitor_source(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "skill"
            install(ROOT, target)
            deployed = json.loads((target / "src/clusterflow/data/group-cluster.json").read_text(encoding="utf-8"))
            self.assertEqual(deployed, site_profile())
            self.assertTrue((target / "references/group-cluster.md").is_file())
            self.assertTrue((target / "src/clusterflow/joblist.py").is_file())

    def test_site_init_prefills_group_host_but_uses_personal_auth_and_trust(self):
        from types import SimpleNamespace
        key = SimpleNamespace(asbytes=lambda: b"fake-site-key", get_name=lambda: "ssh-ed25519")
        args = make_parser().parse_args(["init", "--site", "group-cluster", "--user", "member", "--auth", "agent"])
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "config")
            with patch("clusterflow.transport.probe_key", return_value=key), \
                 patch("clusterflow.transport.host_key_known", return_value=False), \
                 patch("clusterflow.transport.trust_key") as trust, \
                 patch("clusterflow.transport.Remote") as remote, \
                 patch("sys.stdin.isatty", return_value=True), patch("builtins.input", return_value="yes") as prompt, \
                 contextlib.redirect_stderr(io.StringIO()):
                remote.return_value.__enter__.return_value.exec.return_value = (0, "", "")
                result = setup(store, args)
            profile = store.get("clusters", "Cluster")
            self.assertEqual(profile["host"], "10.0.43.7")
            self.assertEqual(profile["user"], "member")
            self.assertEqual(profile["site"], "group-cluster")
            self.assertEqual(result["host_trust"], "first_use_confirmation")
            prompt.assert_called_once()
            trust.assert_called_once()

    def test_site_init_explicit_host_and_alias_win(self):
        args = make_parser().parse_args(["init", "custom", "--site", "group-cluster", "--host", "example.invalid", "--user", "member", "--auth", "agent"])
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            with patch("clusterflow.transport.probe_key", return_value=object()), \
                 patch("clusterflow.transport.host_key_known", return_value=True), \
                 patch("clusterflow.transport.Remote") as remote:
                remote.return_value.__enter__.return_value.exec.return_value = (0, "", "")
                setup(store, args)
            self.assertEqual(store.get("clusters", "custom")["host"], "example.invalid")


if __name__ == "__main__":
    unittest.main()

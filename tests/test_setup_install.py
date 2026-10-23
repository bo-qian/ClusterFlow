"""Configuration, key trust, secure storage and complete skill deployment."""

import argparse
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
from clusterflow.cli import hidden_password, setup, main
from clusterflow.config import cluster_profile, validate_project
from clusterflow.installer import install
from clusterflow.storage import FlowError, Store, lock, read_json, relative_path, remote_path
from clusterflow.transport import credential_id, fingerprint, password_get, password_set, trust_key


class SetupInstallTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="clusterflow-setup-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.store = Store(self.root / "private")

    def test_config_is_not_created_on_read(self):
        self.assertEqual(self.store.names("projects"), [])
        self.assertFalse(self.store.home.exists())

    def test_atomic_config_and_explicit_replace(self):
        self.store.put("clusters", "test", {"host": "example.invalid"})
        with self.assertRaises(FlowError):
            self.store.put("clusters", "test", {})
        self.assertEqual(self.store.get("clusters", "test")["host"], "example.invalid")

    def test_lock_excludes_concurrent_operations(self):
        path = self.root / "operation.lock"
        with lock(path):
            with self.assertRaises(FlowError):
                with lock(path):
                    pass
        self.assertFalse(path.exists())

    def test_path_validation(self):
        for path in ("/", "relative", "/home/u/../other", "/home/u\ncommand"):
            with self.subTest(path=path), self.assertRaises(FlowError):
                remote_path(path)
        for path in ("../file", "/etc/passwd", "C:/file", "a\\b"):
            with self.subTest(path=path), self.assertRaises(FlowError):
                relative_path(path)

    def test_auth_profile_has_no_password_field(self):
        profile = cluster_profile("test", "example.invalid", "researcher", auth="password")
        self.assertNotIn("password", profile)
        changed = dict(profile, host="different.invalid")
        self.assertNotEqual(credential_id(profile), credential_id(changed))

    def test_noninteractive_password_is_rejected(self):
        with patch("sys.stdin.isatty", return_value=False), self.assertRaises(FlowError):
            hidden_password()

    def test_password_echo_fallback_is_forbidden(self):
        import getpass
        import warnings
        def unsupported_terminal(*args):
            warnings.warn("cannot hide input", getpass.GetPassWarning)
            raise AssertionError("must stop before reading an echoed password")
        with patch("sys.stdin.isatty", return_value=True), patch("getpass.getpass", side_effect=unsupported_terminal), self.assertRaises(FlowError):
            hidden_password()

    def test_password_uses_backend_and_is_not_written_to_json(self):
        profile = cluster_profile("test", "example.invalid", "researcher", auth="password")
        saved = {}
        class Backend:
            def set_password(self, service, name, value):
                saved[(service, name)] = value
            def get_password(self, service, name):
                return saved.get((service, name))
        with patch("clusterflow.transport.credential_backend", return_value=Backend()):
            password_set(profile, "fake-test-password")
            self.assertEqual(password_get(profile), "fake-test-password")
        self.store.put("clusters", "test", profile)
        self.assertNotIn("fake-test-password", (self.store.home / "clusters/test.json").read_text())

    def test_unavailable_keyring_never_writes_plaintext(self):
        profile = cluster_profile("test", "example.invalid", "researcher", auth="password")
        with patch("clusterflow.transport.credential_backend", side_effect=FlowError("unavailable")):
            with self.assertRaises(FlowError):
                password_set(profile, "test-only")
        self.assertFalse(self.store.home.exists())

    def test_host_key_must_match_and_change_is_rejected(self):
        try:
            import paramiko
        except ImportError:
            self.skipTest("Paramiko not installed in this optional source-only test environment")
        key = paramiko.RSAKey.generate(1024)
        profile = cluster_profile("test", "example.invalid", "researcher", auth="agent")
        with self.assertRaises(FlowError):
            trust_key(self.store, profile, key, "SHA256:wrong")
        self.assertFalse((self.store.home / "known_hosts").exists())
        trust_key(self.store, profile, key, fingerprint(key))
        changed = paramiko.RSAKey.generate(1024)
        with self.assertRaises(FlowError):
            trust_key(self.store, profile, changed, fingerprint(changed))
        keys = paramiko.HostKeys(str(self.store.home / "known_hosts"))
        self.assertEqual(keys["example.invalid"][key.get_name()], key)

    def test_setup_with_fake_ssh_keeps_password_out_of_config(self):
        from types import SimpleNamespace
        key = SimpleNamespace(asbytes=lambda: b"test server public key")
        class Connection:
            def __init__(self, *args, **kwargs):
                self.password = kwargs.get("password")
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def exec(self, args):
                return 0, "", ""
        args = argparse.Namespace(name="test", host="example.invalid", user="user", port=22, auth="password", key_file=None, jump=None, fingerprint=fingerprint(key), replace=False)
        with patch("clusterflow.transport.credential_backend"), patch("clusterflow.transport.probe_key", return_value=key), patch("clusterflow.transport.trust_key"), patch("clusterflow.transport.Remote", Connection), patch("clusterflow.transport.password_set") as setter, patch("clusterflow.cli.hidden_password", return_value="test-only-secret"):
            report = setup(self.store, args)
            setter.assert_called_once()
        self.assertTrue(report["connection_verified"])
        self.assertNotIn("test-only-secret", (self.store.home / "clusters/test.json").read_text())

    def test_install_contains_whole_skill_and_tool(self):
        target = self.root / "skills/clusterflow"
        result = install(ROOT, target)
        self.assertFalse(result["registered"])
        for path in ("SKILL.md", "scripts/clusterflow.py", "src/clusterflow/cli.py", "references/workflow.md", "examples/project.example.json"):
            self.assertTrue((target / path).is_file())
        self.assertFalse((target / ".git").exists())
        self.assertFalse((target / "tests").exists())

    def test_update_keeps_recoverable_backup(self):
        target = self.root / "skills/clusterflow"
        install(ROOT, target)
        result = install(ROOT, target, update=True)
        self.assertTrue(Path(result["backup"], "SKILL.md").is_file())

    def test_modified_install_is_not_overwritten(self):
        target = self.root / "skills/clusterflow"
        install(ROOT, target)
        (target / "SKILL.md").write_text("user changes", encoding="utf-8")
        with self.assertRaises(FlowError):
            install(ROOT, target, update=True)
        self.assertEqual((target / "SKILL.md").read_text(), "user changes")

    def test_nonmanaged_target_is_not_overwritten(self):
        target = self.root / "existing"
        target.mkdir()
        (target / "user.txt").write_text("keep")
        with self.assertRaises(FlowError):
            install(ROOT, target)
        self.assertEqual((target / "user.txt").read_text(), "keep")

    def test_source_ancestor_cannot_be_install_target(self):
        with self.assertRaises(FlowError):
            install(ROOT, ROOT.parent)

    def test_missing_config_command_fails_safely(self):
        with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(["--home", str(self.root / "empty"), "submit", "missing", "missing", "--yes"]), 1)


if __name__ == "__main__":
    unittest.main()

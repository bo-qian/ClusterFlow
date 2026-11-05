"""First-use onboarding and persistent host pins with fake SSH/keyring only."""

import contextlib
import io
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from clusterflow.cli import make_parser, setup
from clusterflow.config import cluster_profile
from clusterflow.storage import FlowError, Store
from clusterflow.transport import fingerprint, host_key_known, trust_key


class HostTrustTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            import paramiko
        except ImportError:
            raise unittest.SkipTest("Host trust tests require Paramiko")
        cls.key = paramiko.RSAKey.generate(1024)
        cls.changed_key = paramiko.RSAKey.generate(1024)
        cls.other_algorithm = paramiko.ECDSAKey.generate()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="clusterflow-trust-")
        self.addCleanup(temporary.cleanup)
        self.store = Store(Path(temporary.name) / "private")
        self.args = make_parser().parse_args([
            "init", "test", "--host", "example.invalid", "--user", "researcher", "--auth", "agent",
        ])
        self.profile = cluster_profile("test", "example.invalid", "researcher", auth="agent")
        self.output = io.StringIO()
        stack = contextlib.ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(contextlib.redirect_stdout(self.output))
        stack.enter_context(contextlib.redirect_stderr(self.output))
        self.probe = stack.enter_context(patch("clusterflow.transport.probe_key", return_value=self.key))
        self.remote = stack.enter_context(patch("clusterflow.transport.Remote"))
        self.remote.return_value.__enter__.return_value.exec.return_value = (0, "", "")
        stack.enter_context(patch("clusterflow.transport.credential_backend"))
        self.password = stack.enter_context(patch("clusterflow.cli.hidden_password", return_value="fake-secret"))
        self.save_password = stack.enter_context(patch("clusterflow.transport.password_set"))
        self.terminal = stack.enter_context(patch("sys.stdin.isatty", return_value=True))
        self.prompt = stack.enter_context(patch("builtins.input", return_value="y"))

    def pin(self):
        trust_key(self.store, self.profile, self.key, fingerprint(self.key))
        return (self.store.home / "known_hosts").read_bytes()

    def assert_not_authenticated(self):
        self.remote.assert_not_called()
        self.password.assert_not_called()
        self.save_password.assert_not_called()
        self.assertEqual(self.store.names("clusters"), [])

    def test_default_confirms_once_and_pins_before_hidden_password(self):
        self.args.auth = "password"
        self.prompt.return_value = " YES "

        def password_after_pin():
            self.assertTrue(host_key_known(self.store, self.profile, self.key))
            self.remote.assert_not_called()
            return "fake-secret"

        self.password.side_effect = password_after_pin
        report = setup(self.store, self.args)
        self.prompt.assert_called_once()
        self.password.assert_called_once()
        self.save_password.assert_called_once()
        self.remote.return_value.__enter__.return_value.exec.assert_called_once_with(["true"])
        self.assertTrue(report["connection_verified"])
        self.assertTrue(report["credential_saved"])
        self.assertEqual(report["host_trust"], "first_use_confirmation")
        self.assertIn("not independently verified", self.output.getvalue())
        self.assertIn(fingerprint(self.key), self.output.getvalue())
        self.assertNotIn("fake-secret", self.output.getvalue())
        for path in self.store.home.rglob("*"):
            if path.is_file():
                self.assertNotIn(b"fake-secret", path.read_bytes())

    def test_decline_empty_or_unrecognized_answer_does_not_pin(self):
        for answer in ("", "n", "no", "sure", fingerprint(self.key)):
            with self.subTest(answer=answer):
                self.prompt.return_value = answer
                with self.assertRaisesRegex(FlowError, "not confirmed"):
                    setup(self.store, self.args)
                self.assertFalse((self.store.home / "known_hosts").exists())
                self.assertFalse((self.store.home / "hostkeys.lock").exists())
                self.assert_not_authenticated()

    def test_eof_or_interrupt_does_not_pin(self):
        for error in (EOFError, KeyboardInterrupt):
            with self.subTest(error=error):
                self.prompt.side_effect = error
                with self.assertRaisesRegex(FlowError, "not confirmed"):
                    setup(self.store, self.args)
                self.assertFalse((self.store.home / "known_hosts").exists())
                self.assert_not_authenticated()

    def test_unknown_host_cannot_be_trusted_through_piped_input(self):
        self.terminal.return_value = False
        with self.assertRaisesRegex(FlowError, "interactive local terminal"):
            setup(self.store, self.args)
        self.prompt.assert_not_called()
        self.assertFalse((self.store.home / "known_hosts").exists())
        self.assert_not_authenticated()

    def test_optional_fingerprint_allows_agent_setup_without_prompt(self):
        self.terminal.return_value = False
        self.args.fingerprint = fingerprint(self.key)
        report = setup(self.store, self.args)
        self.prompt.assert_not_called()
        self.password.assert_not_called()
        self.assertTrue(host_key_known(self.store, self.profile, self.key))
        self.assertEqual(report["host_trust"], "fingerprint_match")

    def test_optional_fingerprint_mismatch_does_not_offer_fallback(self):
        for expected in ("SHA256:wrong", ""):
            with self.subTest(expected=expected):
                self.args.fingerprint = expected
                with self.assertRaisesRegex(FlowError, "expected fingerprint"):
                    setup(self.store, self.args)
                self.prompt.assert_not_called()
                self.assertFalse((self.store.home / "known_hosts").exists())
                self.assert_not_authenticated()

    def test_known_host_reuses_pin_without_prompt_or_rewrite(self):
        previous = self.pin()
        self.terminal.return_value = False
        report = setup(self.store, self.args)
        self.prompt.assert_not_called()
        self.assertEqual(report["host_trust"], "stored_key")
        self.assertEqual((self.store.home / "known_hosts").read_bytes(), previous)

    def test_known_host_still_checks_explicit_fingerprint(self):
        previous = self.pin()
        self.args.fingerprint = "SHA256:wrong"
        with self.assertRaisesRegex(FlowError, "expected fingerprint"):
            setup(self.store, self.args)
        self.prompt.assert_not_called()
        self.assert_not_authenticated()
        self.assertEqual((self.store.home / "known_hosts").read_bytes(), previous)

    def test_changed_key_or_algorithm_blocks_even_replace_and_matching_fingerprint(self):
        previous = self.pin()
        self.store.put("clusters", "test", self.profile)
        self.args.replace = True
        for key in (self.changed_key, self.other_algorithm):
            for expected in (None, fingerprint(key)):
                with self.subTest(algorithm=key.get_name(), fingerprint=expected):
                    self.probe.return_value = key
                    self.args.fingerprint = expected
                    with self.assertRaisesRegex(FlowError, "Stored host key changed"):
                        setup(self.store, self.args)
                    self.prompt.assert_not_called()
                    self.password.assert_not_called()
                    self.remote.assert_not_called()
                    self.assertEqual((self.store.home / "known_hosts").read_bytes(), previous)
                    self.assertEqual(self.store.get("clusters", "test"), self.profile)

    def test_nondefault_port_has_independent_pin(self):
        self.pin()
        self.args.port = 2222
        self.probe.return_value = self.changed_key
        report = setup(self.store, self.args)
        self.prompt.assert_called_once()
        self.assertEqual(report["host_trust"], "first_use_confirmation")
        self.assertIn("[example.invalid]:2222", self.output.getvalue())
        self.assertTrue(host_key_known(self.store, self.profile, self.key))
        self.assertTrue(host_key_known(self.store, dict(self.profile, port=2222), self.changed_key))

    def test_alias_for_same_endpoint_reuses_pin(self):
        self.pin()
        self.args.name = "other"
        self.terminal.return_value = False
        report = setup(self.store, self.args)
        self.prompt.assert_not_called()
        self.assertEqual(report["host_trust"], "stored_key")

    def test_failed_readonly_verification_never_saves_profile_or_password(self):
        self.args.auth = "password"
        self.remote.return_value.__enter__.return_value.exec.return_value = (1, "", "failed")
        with self.assertRaisesRegex(FlowError, "connection verification failed"):
            setup(self.store, self.args)
        self.save_password.assert_not_called()
        self.assertEqual(self.store.names("clusters"), [])

    def test_auth_connection_failure_does_not_save_profile_or_password(self):
        self.args.auth = "password"
        self.remote.side_effect = FlowError("SSH authentication failed")
        with self.assertRaisesRegex(FlowError, "authentication failed"):
            setup(self.store, self.args)
        self.save_password.assert_not_called()
        self.assertEqual(self.store.names("clusters"), [])

    def test_system_store_failure_stops_before_probe_or_password(self):
        self.args.auth = "password"
        with patch("clusterflow.transport.credential_backend", side_effect=FlowError("store unavailable")):
            with self.assertRaisesRegex(FlowError, "store unavailable"):
                setup(self.store, self.args)
        self.probe.assert_not_called()
        self.prompt.assert_not_called()
        self.assert_not_authenticated()


if __name__ == "__main__":
    unittest.main()

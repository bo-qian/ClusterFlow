"""Observable offline behavior; no network, credentials or user config needed."""

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from clusterflow import __version__
from clusterflow.cli import main


class CliTests(unittest.TestCase):
    def invoke(self, arguments):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = main(arguments)
        return code, output.getvalue()

    def test_status_reports_real_capabilities(self):
        code, output = self.invoke(["status", "--json"])
        report = json.loads(output)
        self.assertEqual(code, 0)
        self.assertIn("submit", report["implemented"])
        self.assertIn("init", report["implemented"])
        self.assertFalse(report["offline_only"])
        self.assertFalse(report["remote_verified"])
        self.assertEqual(report["planned"], [])

    def test_doctor_only_claims_local_runtime(self):
        code, output = self.invoke(["doctor", "--json"])
        report = json.loads(output)
        self.assertEqual(code, 0 if report["ok"] else 1)
        self.assertEqual([check["name"] for check in report["checks"]], ["python", "paramiko", "keyring"])
        self.assertIn("credentials", report["not_checked"])
        self.assertIn("numerical_regression", report["not_checked"])

    def test_plain_output(self):
        for command in ("status", "doctor"):
            code, output = self.invoke([command])
            self.assertIn(code, (0, 1))
            self.assertIn("ClusterFlow", output)

    def test_unimplemented_commands_fail_without_fallback(self):
        for command in ("unsafe-exec", "unimplemented", "delete-remote", "password-dump"):
            with self.subTest(command=command), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    main([command])
                self.assertEqual(error.exception.code, 2)

    def test_help_and_version_exit_successfully(self):
        for arguments in (["--help"], ["--version"]):
            with contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    main(arguments)
            self.assertEqual(error.exception.code, 0)

    def test_environment_secret_is_not_echoed(self):
        marker = "test-only-should-not-appear-in-output"
        with patch.dict(os.environ, {"TOOLBOX_SFTP_PASSWORD": marker}):
            for command in ("status", "doctor"):
                _, output = self.invoke([command, "--json"])
                self.assertNotIn(marker, output)

    def test_commands_do_not_open_sockets(self):
        with patch("socket.socket", side_effect=AssertionError("network forbidden")):
            self.invoke(["status", "--json"])
            self.invoke(["doctor", "--json"])

    def test_checkout_entry_works_from_other_directory_without_writes(self):
        with tempfile.TemporaryDirectory(prefix="clusterflow-test-") as directory:
            for command in ("status", "doctor"):
                result = subprocess.run(
                    [sys.executable, str(ROOT / "scripts" / "clusterflow.py"), command, "--json"],
                    cwd=directory, capture_output=True, text=True, check=False,
                )
                report = json.loads(result.stdout)
                self.assertEqual(result.returncode, 0 if report.get("ok", True) else 1)
                self.assertIn("offline_only", report)
                if command == "doctor":
                    self.assertTrue(report["offline_only"])
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_example_is_valid_toml_without_embedded_secret(self):
        profile = json.loads((ROOT / "examples" / "project.example.json").read_text())
        self.assertEqual(profile["cluster"], "example-cluster")
        self.assertNotIn("password", profile)
        self.assertTrue(profile["check_command"])

    def test_package_version_matches_build_metadata(self):
        with (ROOT / "pyproject.toml").open("rb") as stream:
            metadata = tomllib.load(stream)
        self.assertEqual(metadata["project"]["version"], __version__)


if __name__ == "__main__":
    unittest.main()

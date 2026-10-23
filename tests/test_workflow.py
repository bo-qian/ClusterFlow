"""End-to-end behavior with a fake transport and Slurm, never a live cluster."""

import json
from pathlib import Path
import stat
import sys
import tempfile
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from clusterflow.config import validate_project
from clusterflow import jobs
from clusterflow.snapshot import prepare, verify
from clusterflow.storage import FlowError, execution_path, record


class FakeRemote:
    def __init__(self):
        self.files = {}
        self.directories = {"/"}
        self.calls = []
        self.submissions = 0
        self.job = None
        self.state = "PENDING"
        self.held = True
        self.lose_response = False
        self.reject = False
        self.node_state = "idle"

    def exists(self, path):
        return path in self.files or path in self.directories

    def mkdirs(self, path):
        self.directories.add(path)
        self.directories.update(str(parent) for parent in Path(path).parents)

    def read(self, path, limit=64 * 1024 * 1024):
        data = self.files[path]
        if len(data) > limit:
            raise FlowError("limit")
        return data

    def write(self, path, data, mode=0o600):
        if path in self.files:
            raise FlowError("overwrite")
        self.mkdirs(path.rsplit("/", 1)[0])
        self.files[path] = data

    def list(self, path):
        prefix = path + "/"
        result = {}
        for name, data in self.files.items():
            if name.startswith(prefix):
                part = name[len(prefix):].split("/")[0]
                directory = "/" in name[len(prefix):]
                result[part] = SimpleNamespace(filename=part, st_mode=stat.S_IFDIR if directory else stat.S_IFREG, st_size=0 if directory else len(data))
        return list(result.values())

    def seal(self, path):
        self.sealed = path

    def exec(self, arguments, timeout=60):
        self.calls.append(arguments)
        if arguments[0] == "sinfo":
            return 0, f"node1|cpu*|{self.node_state}|64|256000\n", ""
        if arguments[0] == "sbatch":
            self.submissions += 1
            if self.reject:
                return 1, "", "rejected"
            self.job = "1234"
            self.name = next(value.split("=", 1)[1] for value in arguments if value.startswith("--job-name="))
            if self.lose_response:
                raise FlowError("response lost")
            return 0, self.job + ";example\n", ""
        if arguments[:3] == ["scontrol", "show", "job"]:
            return 0, f"JobId=1234 JobName={self.name} JobState={self.state} Reason={'JobHeldUser' if self.held else 'None'} TimeLimit=UNLIMITED", ""
        if arguments[:2] == ["scontrol", "release"]:
            self.held = False
            self.state = "RUNNING"
            return 0, "", ""
        if arguments[0] == "squeue":
            if "--name" in arguments:
                return 0, f"1234|{self.name}\n" if self.job else "", ""
            return 0, f"1234|{self.state}|node1|0:01|UNLIMITED\n" if self.state in ("PENDING", "RUNNING") else "", ""
        if arguments[0] == "sacct":
            if "--name" in arguments:
                return 0, f"1234|{self.name}\n" if self.job else "", ""
            return 0, f"1234|{self.state}|0:0|00:01|UNLIMITED\n", ""
        if arguments[0] == "scancel":
            self.state = "CANCELLED"
            return 0, "", ""
        raise AssertionError(arguments)


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="clusterflow-flow-")
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        source = root / "source with spaces"
        source.mkdir()
        (source / "input.json").write_text('{"steps": 3}', encoding="utf-8")
        (source / "solver.py").write_text("print('simulation example')\n", encoding="utf-8")
        self.project = validate_project({"name": "demo", "cluster": "test", "local_source": str(source), "remote_source": "/home/u/code with spaces", "remote_runs": "/home/u/runs", "local_results": str(root / "results"), "command": ["python", "solver.py", "--input", "{input}"], "check_command": [sys.executable, "-c", "import json; json.load(open('input.json'))"], "resources": {"tasks": 2, "nodes": 1, "memory_mb": 1000, "partition": "cpu", "time": "0"}})
        self.remote = FakeRemote()

    def prepared(self):
        return prepare(self.project, "input.json")

    def synced(self):
        data = self.prepared()
        return jobs.sync(self.project, data, self.remote)

    def test_full_flow(self):
        data = self.prepared()
        preview = jobs.preview(self.project, data)
        self.assertEqual(preview["deletions"], [])
        self.assertEqual(preview["files"], ["input.json", "solver.py"])
        jobs.sync(self.project, data, self.remote)
        jobs.submit(self.project, data, self.remote)
        self.assertEqual(data["job_id"], "1234")
        self.assertEqual(data["state"], "SUBMITTED")
        self.assertEqual(jobs.job_status(self.remote, "1234")["state"], "RUNNING")
        csv = b"Step,Sim_Time,Energy\n1,0.1,5\n2,0.2,4\n3,0.3"
        self.remote.write(data["remote_run"] + "/data/energy.csv", csv)
        download = jobs.download(self.project, data, self.remote)
        self.assertEqual(len(download["files"]), 1)
        self.assertEqual(jobs.progress(self.project, data)[0]["latest_complete_row"]["Step"], "2")
        jobs.cancel(self.project, data, self.remote)
        self.assertEqual(jobs.job_status(self.remote, "1234")["state"], "CANCELLED")
        self.assertGreaterEqual(len(record(self.project, data["execution_id"])["events"]), 5)

    def test_original_source_edits_do_not_change_snapshot(self):
        data = self.prepared()
        Path(self.project["local_source"], "solver.py").write_text("changed")
        self.assertIn("solver.py", verify(self.project, data)["files"])

    def test_tampered_snapshot_blocks_sync(self):
        data = self.prepared()
        (execution_path(self.project, data["execution_id"]) / "source/solver.py").write_text("tampered")
        with self.assertRaises(FlowError):
            jobs.sync(self.project, data, self.remote)
        self.assertEqual(self.remote.files, {})

    def test_failed_check_is_not_prepared(self):
        self.project["check_command"] = [sys.executable, "-c", "raise SystemExit(5)"]
        with self.assertRaises(FlowError):
            self.prepared()
        self.assertEqual(list(Path(self.project["record_root"]).rglob("record.json")), [])

    def test_check_cannot_rewrite_input(self):
        self.project["check_command"] = [sys.executable, "-c", "open('input.json','w').write('{}')"]
        with self.assertRaises(FlowError):
            self.prepared()

    def test_check_timeout_cannot_prepare_submission(self):
        import subprocess
        from unittest.mock import patch
        with patch("clusterflow.snapshot.subprocess.run", side_effect=subprocess.TimeoutExpired("check", 1)), self.assertRaises(FlowError):
            self.prepared()

    def test_secret_and_large_outputs_excluded(self):
        source = Path(self.project["local_source"])
        (source / ".env").write_text("PRIVATE=example")
        (source / "field.vtu").write_text("bulk")
        (source / "outputs").mkdir()
        (source / "outputs/data.csv").write_text("bulk")
        data = self.prepared()
        self.assertEqual(set(verify(self.project, data)["files"]), {"input.json", "solver.py"})

    def test_embedded_private_key_blocks_prepare(self):
        Path(self.project["local_source"], "unsafe.txt").write_text("-----BEGIN PRIVATE KEY-----\nexample")
        with self.assertRaises(FlowError):
            self.prepared()

    def test_remote_conflict_is_preserved(self):
        data = self.synced()
        path = data["remote_code"] + "/solver.py"
        self.remote.files[path] = b"remote edit"
        with self.assertRaises(FlowError):
            jobs.sync(self.project, data, self.remote)
        self.assertEqual(self.remote.files[path], b"remote edit")

    def test_sync_is_idempotent(self):
        data = self.synced()
        before = dict(self.remote.files)
        jobs.sync(self.project, data, self.remote)
        self.assertEqual(before, self.remote.files)

    def test_submit_rechecks_remote_source(self):
        data = self.synced()
        self.remote.files[data["remote_code"] + "/solver.py"] = b"changed"
        with self.assertRaises(FlowError):
            jobs.submit(self.project, data, self.remote)
        self.assertEqual(self.remote.submissions, 0)

    def test_duplicate_submit_is_blocked(self):
        data = self.synced()
        jobs.submit(self.project, data, self.remote)
        with self.assertRaises(FlowError):
            jobs.submit(self.project, data, self.remote)
        self.assertEqual(self.remote.submissions, 1)

    def test_lost_response_reconciles_without_resubmitting(self):
        data = self.synced()
        self.remote.lose_response = True
        with self.assertRaises(FlowError):
            jobs.submit(self.project, data, self.remote)
        data = record(self.project, data["execution_id"])
        self.assertEqual(data["state"], "SUBMIT_UNKNOWN")
        with self.assertRaises(FlowError):
            jobs.submit(self.project, data, self.remote)
        jobs.reconcile(self.project, data, self.remote)
        self.assertEqual(data["job_id"], "1234")
        self.assertTrue(self.remote.held)
        jobs.release(self.project, data, self.remote)
        self.assertEqual(self.remote.submissions, 1)

    def test_unresolved_submission_does_not_reset(self):
        data = self.synced()
        data["state"] = "SUBMIT_UNKNOWN"
        with self.assertRaises(FlowError):
            jobs.reconcile(self.project, data, self.remote)
        self.assertEqual(data["state"], "SUBMIT_UNKNOWN")

    def test_bad_node_blocks_before_sbatch(self):
        data = self.synced()
        self.remote.node_state = "drain*"
        with self.assertRaises(FlowError):
            jobs.submit(self.project, data, self.remote)
        self.assertEqual(self.remote.submissions, 0)

    def test_no_matching_capacity_blocks_submit(self):
        self.project["resources"]["memory_mb"] = 999999
        data = self.synced()
        with self.assertRaises(FlowError):
            jobs.submit(self.project, data, self.remote)

    def test_no_automatic_finite_walltime(self):
        del self.project["resources"]["time"]
        data = self.synced()
        jobs.submit(self.project, data, self.remote)
        args = next(args for args in self.remote.calls if args[0] == "sbatch")
        self.assertFalse(any(value.startswith("--time=") for value in args))

    def test_download_limit_prevents_partial_publication(self):
        data = self.synced()
        self.remote.write(data["remote_run"] + "/data/large.csv", b"12345")
        with self.assertRaises(FlowError):
            jobs.download(self.project, data, self.remote, max_bytes=4)
        self.assertFalse(Path(self.project["local_results"], data["run_id"], data["execution_id"], "data/large.csv").exists())

    def test_only_explicit_groups_download_large_fields(self):
        data = self.synced()
        self.remote.write(data["remote_run"] + "/fields/field.vtu", b"123")
        self.assertEqual(jobs.download(self.project, data, self.remote)["files"], [])
        self.assertEqual(len(jobs.download(self.project, data, self.remote, groups=["fields"])["files"]), 1)

    def test_continuation_keeps_logical_run(self):
        data = self.synced()
        jobs.submit(self.project, data, self.remote)
        self.remote.state = "COMPLETED"
        continuation = prepare(self.project, "input.json", resume=data["execution_id"])
        self.assertEqual(continuation["run_id"], data["run_id"])
        self.assertNotEqual(continuation["execution_id"], data["execution_id"])
        self.assertEqual(continuation["parent_execution"], data["execution_id"])

    def test_path_traversal_cannot_select_input(self):
        with self.assertRaises(FlowError):
            prepare(self.project, "../outside.json")

    def test_changed_launch_settings_block_submission(self):
        data = self.synced()
        data["project_config"]["command"] = ["different-solver"]
        with self.assertRaises(FlowError):
            jobs.submit(self.project, data, self.remote)
        self.assertEqual(self.remote.submissions, 0)

    def test_download_override_preserves_recorded_run_directory(self):
        data = self.synced()
        original = data["remote_run"]
        self.remote.write("/explicit/output/data/values.csv", b"Step,Energy\n1,2\n")
        jobs.download(self.project, data, self.remote, remote_dir="/explicit/output")
        self.assertEqual(record(self.project, data["execution_id"])["remote_run"], original)

    def test_running_parent_cannot_be_continued(self):
        data = self.synced()
        jobs.submit(self.project, data, self.remote)
        child = prepare(self.project, "input.json", resume=data["execution_id"])
        jobs.sync(self.project, child, self.remote)
        with self.assertRaises(FlowError):
            jobs.submit(self.project, child, self.remote)
        self.assertEqual(self.remote.submissions, 1)

    def test_reproduce_uses_old_code_not_changed_worktree(self):
        original = self.prepared()
        Path(self.project["local_source"], "solver.py").write_text("new behavior")
        duplicate = prepare(self.project, "input.json", source_execution=original["execution_id"])
        copied = execution_path(self.project, duplicate["execution_id"]) / "source/solver.py"
        self.assertEqual(copied.read_text(), "print('simulation example')\n")
        self.assertEqual(duplicate["reproduces_execution"], original["execution_id"])
        self.assertNotEqual(duplicate["run_id"], original["run_id"])


if __name__ == "__main__":
    unittest.main()

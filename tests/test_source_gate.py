"""Version checks include output code, without silently deploying solver files."""

from pathlib import Path
import unittest

import test_workflow as fixtures
from test_remote_inputs import InputRemote
from clusterflow import jobs
from clusterflow.config import validate_project
from clusterflow.snapshot import prepare, verify
from clusterflow.storage import FlowError


class RequiredSourceTests(unittest.TestCase):
    def setUp(self):
        fixtures.WorkflowTests.setUp(self)
        root = Path(self.project["local_source"])
        (root / "plot.py").write_text("print('selected output style')\n")
        self.project.update(code_mode="remote", remote_check_command=["validate-input", "{input}"], required_source_files=["solver.py", "plot.py"])
        self.project = validate_project(self.project)
        self.remote = InputRemote()
        for rel in self.project["required_source_files"]:
            self.remote.write(self.project["remote_source"] + "/" + rel, (root / rel).read_bytes())

    def test_matching_source_passes_without_uploading_code(self):
        data = prepare(self.project, "input.json")
        self.assertEqual(set(verify(self.project, data)["files"]), {"input.json"})
        before = dict(self.remote.files)
        jobs.sync(self.project, data, self.remote)
        jobs.submit(self.project, data, self.remote)
        self.assertEqual(data["required_source_check"]["mismatches"], [])
        self.assertEqual(set(self.remote.files) - set(before), {jobs.input_location(data), data["remote_run"] + "/submit.sh"})
        for path, payload in before.items():
            self.assertEqual(self.remote.files[path], payload)

    def test_old_output_code_blocks_before_input_upload(self):
        data = prepare(self.project, "input.json")
        self.remote.files[self.project["remote_source"] + "/plot.py"] = b"old output style"
        with self.assertRaisesRegex(FlowError, "plot.py"):
            jobs.sync(self.project, data, self.remote)
        self.assertFalse(self.remote.exists(jobs.input_location(data)))
        self.assertEqual(self.remote.submissions, 0)

    def test_changed_untracked_output_code_blocks_submission(self):
        data = prepare(self.project, "input.json")
        jobs.sync(self.project, data, self.remote)
        self.remote.files[self.project["remote_source"] + "/plot.py"] = b"changed after sync"
        with self.assertRaisesRegex(FlowError, "plot.py"):
            jobs.submit(self.project, data, self.remote)
        self.assertEqual(self.remote.submissions, 0)

    def test_missing_remote_source_blocks_sync(self):
        data = prepare(self.project, "input.json")
        del self.remote.files[self.project["remote_source"] + "/plot.py"]
        with self.assertRaisesRegex(FlowError, "plot.py"):
            jobs.sync(self.project, data, self.remote)

    def test_missing_local_source_fails_prepare(self):
        self.project["required_source_files"].append("missing.py")
        with self.assertRaisesRegex(FlowError, "missing.py"):
            prepare(self.project, "input.json")

    def test_invalid_source_selection_rejected(self):
        for paths in ([], "solver.py", ["../other.py"], ["/absolute.py"], [1]):
            project = dict(self.project, required_source_files=paths)
            with self.subTest(paths=paths), self.assertRaises(FlowError):
                validate_project(project)

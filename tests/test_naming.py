"""Scheduler labels must stay readable, immutable and safe for old executions."""

import json
from pathlib import Path
import unittest

import test_workflow as fixtures
from clusterflow import jobs
from clusterflow.cli import make_parser
from clusterflow.naming import readable_label
from clusterflow.snapshot import prepare, verify
from clusterflow.storage import FlowError, digest, execution_path, record, write_json


class NamingTests(unittest.TestCase):
    def setUp(self):
        fixtures.WorkflowTests.setUp(self)

    def test_explicit_label_is_frozen_and_previewed(self):
        data = prepare(self.project, "input.json", label="MODEL_G1_240x160_dt0p01")
        self.assertEqual(data["label"], "MODEL_G1_240x160_dt0p01")
        self.assertEqual(data["job_name"], data["label"] + "_" + data["execution_id"].rsplit("_", 1)[1])
        self.assertEqual(jobs.preview(self.project, data)["job_name"], data["job_name"])
        self.assertNotEqual(data["run_id"], data["label"])
        self.assertEqual(make_parser().parse_args(["prepare", "demo", "--input", "input.json", "--label", data["label"]]).label, data["label"])

    def test_default_label_uses_project_and_input(self):
        data = prepare(self.project, "input.json")
        self.assertEqual(data["label"], "demo_input")

    def test_same_labels_have_distinct_scheduler_identities(self):
        first = prepare(self.project, "input.json", label="same_case")
        second = prepare(self.project, "input.json", label="same_case")
        self.assertNotEqual(jobs.job_name(first), jobs.job_name(second))

    def test_invalid_labels_fail_before_preparation(self):
        for label in ("", "a b", "a\n--other", "a/b", "../case", "-case", "算例", "x" * 65):
            with self.subTest(label=label), self.assertRaises(FlowError):
                prepare(self.project, "input.json", label=label)
        self.assertFalse((Path(self.project["record_root"]) / "executions").exists())

    def test_default_label_has_bounded_portable_name(self):
        label = readable_label(self.project, "inputs/比较 " + "a" * 100 + ".toml")
        self.assertLessEqual(len(label), 64)
        self.assertRegex(label, r"^[A-Za-z0-9][A-Za-z0-9_.-]+$")

    def test_labels_do_not_rewrite_application_input(self):
        source = Path(self.project["local_source"], "input.json")
        source.write_text('{"run_id": "application-owned", "steps": 3}')
        before = source.read_bytes()
        data = prepare(self.project, "input.json", label="readable_name")
        self.assertEqual(source.read_bytes(), before)
        self.assertEqual((execution_path(self.project, data["execution_id"]) / "source/input.json").read_bytes(), before)

    def test_naming_metadata_cannot_be_retagged_after_checks(self):
        for key in ("label", "job_name"):
            data = prepare(self.project, "input.json")
            data[key] = "different"
            with self.subTest(key=key), self.assertRaises(FlowError):
                verify(self.project, data)

    def test_explicit_name_used_in_lost_response_reconciliation(self):
        data = prepare(self.project, "input.json", label="MODEL_time_dt0p1")
        jobs.sync(self.project, data, self.remote)
        self.remote.lose_response = True
        with self.assertRaises(FlowError):
            jobs.submit(self.project, data, self.remote)
        data = record(self.project, data["execution_id"])
        jobs.reconcile(self.project, data, self.remote)
        jobs.release(self.project, data, self.remote)
        self.assertEqual(self.remote.name, data["job_name"])
        self.assertEqual(self.remote.submissions, 1)

    def test_prefix_collision_cannot_authorize_cancel_or_release(self):
        data = prepare(self.project, "input.json", label="MODEL_case")
        jobs.sync(self.project, data, self.remote)
        jobs.submit(self.project, data, self.remote)
        self.remote.name += "_other"
        with self.assertRaises(FlowError):
            jobs.cancel(self.project, data, self.remote)
        self.remote.state, self.remote.held = "PENDING", True
        with self.assertRaises(FlowError):
            jobs.release(self.project, data, self.remote)
        self.assertFalse(any(args[0] == "scancel" for args in self.remote.calls))

    def test_legacy_record_keeps_historical_cf_name(self):
        data = prepare(self.project, "input.json")
        path = execution_path(self.project, data["execution_id"]) / "manifest.json"
        manifest = json.loads(path.read_text())
        for key in ("label", "job_name"):
            del data[key]
            del manifest[key]
        data["manifest_sha256"] = digest(json.dumps(manifest, sort_keys=True).encode())
        write_json(path, manifest)
        jobs.sync(self.project, data, self.remote)
        jobs.submit(self.project, data, self.remote)
        self.assertEqual(self.remote.name, "cf_" + data["execution_id"])
        self.assertEqual(jobs.job_status(self.remote, data["job_id"])["state"], "RUNNING")
        jobs.cancel(self.project, data, self.remote)
        self.assertEqual(self.remote.state, "CANCELLED")

    def test_invalid_recorded_name_never_falls_back_to_other_identity(self):
        with self.assertRaises(FlowError):
            jobs.job_name({"execution_id": "old", "job_name": "bad name"})

"""Typed GPU requests use the frozen resources and fail before sbatch if unsuitable."""

from copy import deepcopy
import unittest

import test_workflow as workflows
from clusterflow.config import validate_project
from clusterflow import jobs
from clusterflow.storage import FlowError


class GPUFake(workflows.FakeRemote):
    rows = "node1|cpu*|idle|64|256000|(null)\nnode5|cpu*|idle|104|750000|gpu:rtx_4090:4(S:0-1)\n"

    def exec(self, arguments, timeout=60):
        if arguments[0] == "sinfo":
            self.calls.append(arguments)
            return 0, self.rows, ""
        return super().exec(arguments, timeout)


class GPUResourceTests(unittest.TestCase):
    def setUp(self):
        self.flow = workflows.WorkflowTests("test_full_flow")
        self.flow.setUp()
        self.addCleanup(self.flow.doCleanups)
        self.project = deepcopy(self.flow.project)
        self.project["resources"] = {"partition": "cpu", "nodelist": "node5", "nodes": 1, "tasks": 1, "cpus_per_task": 8,
                                     "memory_mb": 32768, "gpus_per_node": 1, "gpu_type": "rtx_4090", "time": "00:30:00"}
        self.remote = GPUFake()

    def submit(self):
        from clusterflow.snapshot import prepare
        project = validate_project(self.project)
        data = prepare(project, "input.json")
        jobs.sync(project, data, self.remote)
        return project, data

    def test_typed_gpu_and_cpu_threads_reach_sbatch(self):
        project, data = self.submit()
        jobs.submit(project, data, self.remote)
        command = next(c for c in self.remote.calls if c[0] == "sbatch")
        for flag in ("--gres=gpu:rtx_4090:1", "--cpus-per-task=8", "--nodelist=node5", "--ntasks=1"):
            self.assertIn(flag, command)

    def test_untyped_gpu_count_and_parenthesized_capacities(self):
        self.assertEqual(jobs.gpu_capacity("gpu:rtx_4090:4(S:0-1)"), 4)
        self.assertEqual(jobs.gpu_capacity("gpu:rtx_4090:2,gpu:a100:1", "rtx_4090"), 2)
        self.assertEqual(jobs.gpu_capacity("gpu:4"), 4)
        self.project["resources"].pop("gpu_type")
        project, data = self.submit()
        jobs.submit(project, data, self.remote)
        self.assertIn("--gres=gpu:1", next(c for c in self.remote.calls if c[0] == "sbatch"))

    def test_non_gpu_node_wrong_type_or_excess_count_blocks_submit(self):
        for override in ({"nodelist": "node1"}, {"gpu_type": "a100"}, {"gpus_per_node": 5}, {"nodes": 2}):
            with self.subTest(override=override):
                config = dict(self.project["resources"], **override)
                with self.assertRaises(FlowError):
                    jobs.inspect_resources(self.remote, config)
        self.assertEqual(self.remote.submissions, 0)

    def test_gpu_telemetry_missing_or_drained_is_not_assumed_available(self):
        for rows in ("node5|cpu*|idle|104|750000\n", "node5|cpu*|drain|104|750000|gpu:rtx_4090:4\n"):
            self.remote.rows = rows
            with self.subTest(rows=rows), self.assertRaises(FlowError):
                jobs.inspect_resources(self.remote, self.project["resources"])

    def test_cpus_per_task_is_included_in_capacity_check(self):
        config = dict(self.project["resources"], tasks=2, cpus_per_task=60)
        with self.assertRaises(FlowError):
            jobs.inspect_resources(self.remote, config)

    def test_resource_validation_rejects_invalid_counts_and_type_injection(self):
        for key, value in (("gpus_per_node", 0), ("gpus_per_node", True), ("gpus_per_node", "1"),
                           ("cpus_per_task", -1), ("gpu_type", "rtx:4"), ("gpu_type", "rtx --other")):
            with self.subTest(key=key, value=value), self.assertRaises(FlowError):
                project = deepcopy(self.project)
                project["resources"][key] = value
                validate_project(project)
        project = deepcopy(self.project)
        project["resources"].pop("gpus_per_node")
        with self.assertRaises(FlowError):
            validate_project(project)

    def test_later_project_edits_do_not_change_frozen_gpu_request(self):
        project, data = self.submit()
        project["resources"]["gpus_per_node"] = 4
        jobs.submit(project, data, self.remote)
        command = next(c for c in self.remote.calls if c[0] == "sbatch")
        self.assertIn("--gres=gpu:rtx_4090:1", command)
        self.assertNotIn("--gres=gpu:rtx_4090:4", command)


if __name__ == "__main__":
    unittest.main()

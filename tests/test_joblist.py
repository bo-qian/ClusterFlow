import importlib.util
import contextlib
import io
from pathlib import Path
import re
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('joblist', str(Path(__file__).resolve().parents[1] / 'src' / 'clusterflow' / 'joblist.py'))
j = importlib.util.module_from_spec(spec)
spec.loader.exec_module(j)


class ParsingTests(unittest.TestCase):
    def test_cpu_only_display_has_one_memory_line_and_no_gpu_placeholders(self):
        sample = {'time': 'test', 'cache_seconds': 10, 'memory_scheduling': False, 'select_parameters': 'CR_CPU',
                  'nodes': [{'slurm': {'NodeName': 'node2', 'State': 'IDLE', 'CPUTot': '64', 'CPUAlloc': '0', 'RealMemory': '240000'},
                             'live': {'memory': {'MemTotal': 257060, 'MemAvailable': 250000}}, 'jobs': []}]}
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            j.display(sample)
        text = output.getvalue()
        self.assertEqual(text.count('  MEM:'), 1)
        self.assertNotIn('MEM reservation:', text)
        self.assertNotIn('GPU:', text)
        self.assertNotIn('GPUs on node:', text)

    def test_terminal_colors_preserve_plain_content_and_are_optional(self):
        line = '  CPU: Allocated 12 / Total 64 | Unallocated 52 | Load 1.2'
        colored = j.colorize(line)
        self.assertIn('\033[', colored)
        self.assertEqual(re.sub(r'\033\[[0-9;]*m', '', colored), line)
        self.assertTrue(j.color_enabled('always'))
        self.assertFalse(j.color_enabled('never'))
        with patch.object(j.sys.stdout, 'isatty', return_value=False):
            self.assertFalse(j.color_enabled('auto'))

    def test_gpu_count_and_indices_with_multiple_types(self):
        self.assertEqual(j.gpu_resources('gpu:rtx_4090:2(IDX:0,2),gpu:a100:1(IDX:3)'),
                         {'count': 3, 'indices': [0, 2, 3], 'indices_known': True})
        self.assertEqual(j.gpu_resources('gpu:4(S:0-1)')['count'], 4)
        self.assertEqual(j.gpu_resources('gpu:rtx_4090:0(IDX:N/A)')['indices'], [])

    def test_gpu_job_and_array_zero(self):
        text = ('JobId=10 ArrayJobId=9 ArrayTaskId=0 JobName=idle GPU allocation UserId=alice(1) '
                'JobState=RUNNING NodeList=node5 NumCPUs=8 JOB_GRES=gpu:rtx_4090:2 '
                'Nodes=node5 CPU_IDs=0-7 Mem=0 GRES=gpu:rtx_4090:2(IDX:0,2) MinCPUsNode=1')
        item = j.parse_jobs(text)['node5'][0]
        self.assertEqual(item['id'], '9_0')
        self.assertEqual(item['name'], 'idle GPU allocation')
        self.assertEqual(item['cpus'], 8)
        self.assertEqual(item['gpus']['count'], 2)
        self.assertEqual(item['gpus']['indices'], [0, 2])

    def test_multinode_cpus_are_not_whole_job_cpu_count(self):
        text = ('JobId=11 JobState=RUNNING NodeList=node[2-3] NumCPUs=12 '
                'Nodes=node2 CPU_IDs=0-3 Mem=0 GRES= '
                'Nodes=node3 CPU_IDs=4-11 Mem=0 GRES= MinCPUsNode=1')
        with patch.object(j, 'run', return_value=(0, 'node2\nnode3\n', '')):
            result = j.parse_jobs(text)
        self.assertEqual(result['node2'][0]['cpus'], 4)
        self.assertEqual(result['node3'][0]['cpus'], 8)
        self.assertEqual(result['node2'][0]['gpus']['count'], 0)

    def test_pending_jobs_not_reported_as_allocated(self):
        self.assertEqual(j.parse_jobs('JobId=1 JobState=PENDING NodeList=(null)'), {})

    def test_physical_memory_available_and_gpu_processes(self):
        result = j.parse_probe('''JOBLIST_MEM
MemTotal: 1048576
MemAvailable: 524288
JOBLIST_GPU
0, NVIDIA GeForce RTX 4090, GPU-abc, 0, 100, 49140
JOBLIST_PROCESSES
GPU-abc, 123, 80
JOBLIST_DONE''')
        self.assertEqual(result['memory']['MemAvailable'], 512)
        self.assertEqual(result['gpus'][0]['util_percent'], 0)
        self.assertEqual(result['processes'][0]['uuid'], 'GPU-abc')
        self.assertTrue(result['process_ok'])

    def test_failed_gpu_process_query_not_claimed_idle(self):
        result = j.parse_probe('JOBLIST_PROCESSES\nJOBLIST_PROCESS_ERROR\nJOBLIST_DONE')
        self.assertFalse(result['process_ok'])


if __name__ == '__main__':
    unittest.main()

import unittest
from pathlib import Path
import test_workflow as fixtures
from test_workflow import FakeRemote
from clusterflow import jobs
from clusterflow.config import validate_project
from clusterflow.snapshot import prepare, verify
from clusterflow.storage import FlowError

class InputRemote(FakeRemote):
    revision='abc'
    bad_check=False
    def exec(self,args,timeout=60):
        if args[0]=='test': return 0,'',''
        if args[0] in ('git','bash'): return 0,self.revision,''
        if args[0]=='validate-input': return int(self.bad_check),'',''
        return super().exec(args,timeout)

class RemoteInputTests(unittest.TestCase):
    def setUp(self):
        fixtures.WorkflowTests.setUp(self)
        self.project.update(code_mode='remote',remote_check_command=['validate-input','{input}'])
        self.project=validate_project(self.project)
        self.remote=InputRemote()
    def prepared(self): return prepare(self.project,'input.json')
    def test_only_input_and_lifecycle(self):
        d=self.prepared()
        self.assertEqual(list(verify(self.project,d)['files']),['input.json'])
        Path(self.project['local_source'],'solver.py').write_text('changed locally')
        self.assertFalse(jobs.preview(self.project,d)['source_frozen'])
        jobs.sync(self.project,d,self.remote)
        self.assertEqual(list(self.remote.files),[jobs.input_location(d)])
        self.assertFalse(hasattr(self.remote,'sealed'))
        self.assertEqual(d['remote_code'],self.project['remote_source'])
        self.assertIn(jobs.input_location(d),jobs.batch_script(d).decode())
        jobs.submit(self.project,d,self.remote)
        self.assertEqual(d['state'],'SUBMITTED')
        with self.assertRaises(FlowError): jobs.submit(self.project,d,self.remote)
    def test_failed_check_blocks_submit(self):
        d=self.prepared();self.remote.bad_check=True
        with self.assertRaises(FlowError): jobs.sync(self.project,d,self.remote)
        with self.assertRaises(FlowError): jobs.submit(self.project,d,self.remote)
        self.assertEqual(self.remote.submissions,0)
    def test_conflict(self):
        d=self.prepared();self.remote.files[jobs.input_location(d)]=b'other'
        with self.assertRaises(FlowError): jobs.sync(self.project,d,self.remote)
    def test_code_changed(self):
        d=self.prepared();jobs.sync(self.project,d,self.remote);self.remote.revision='new'
        with self.assertRaises(FlowError): jobs.submit(self.project,d,self.remote)
    def test_input_changed(self):
        d=self.prepared();jobs.sync(self.project,d,self.remote);self.remote.files[jobs.input_location(d)]=b'new'
        with self.assertRaises(FlowError): jobs.submit(self.project,d,self.remote)
    def test_reproduction_blocked(self):
        for kw in ({'resume':'old'},{'source_execution':'old'}):
            with self.assertRaises(FlowError): prepare(self.project,'input.json',**kw)
    def test_missing_checker(self):
        del self.project['remote_check_command']
        with self.assertRaises(FlowError): validate_project(self.project)

    def test_failed_recheck_invalidates_synced_state(self):
        d=self.prepared();jobs.sync(self.project,d,self.remote);self.remote.bad_check=True
        with self.assertRaises(FlowError): jobs.sync(self.project,d,self.remote)
        with self.assertRaises(FlowError): jobs.submit(self.project,d,self.remote)

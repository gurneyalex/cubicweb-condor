from logilab.common.testlib import TestCase, unittest_main

from cubes.condor import commands as condor

class fake_queue:
    def __init__(self, status, output):
        self.status = status
        self.output = output

    def __call__(self, config):
        return self.status, self.output

class CondorTC(TestCase):
    def test_job_ids_normal(self):
        queue_output = """

-- Submitter: xs205803.MELINDA.LOCAL : <10.90.28.11:1923> : xs205803.MELINDA.LOCAL
 ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD
 200.0   EHI528          6/30 11:09   0+00:00:00 I  0   0.0  Python.exe
 201.0   EHI528          6/30 13:20   0+00:00:00 I  0   0.0  Python.exe
 202.0   EHI528          6/30 13:41   0+00:00:00 I  0   0.0  Python.exe

3 jobs; 3 idle, 0 running, 0 held
"""
        condor.queue = fake_queue(0, queue_output)
        self.assertEqual(condor.job_ids(None), ['200.0', '201.0', '202.0'])

    def test_job_ids_empty(self):
        queue_output = """

-- Submitter: xs205803.MELINDA.LOCAL : <10.90.28.11:1923> : xs205803.MELINDA.LOCAL

O jobs; O idle, 0 running, 0 held
"""
        condor.queue = fake_queue(0, queue_output)
        self.assertEqual(condor.job_ids(None), [])

    def test_job_ids_error(self):
        condor.queue = fake_queue(-1, 'No such file or directory')
        self.assertEqual(condor.job_ids(None), [])

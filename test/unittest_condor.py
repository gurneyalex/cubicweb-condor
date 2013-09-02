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

-- Submitter: xs205804.MELINDA.LOCAL : <10.90.28.12:1923> : xs205804.MELINDA.LOCAL
 ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD
 200.0   EHI528          6/30 11:09   0+00:00:00 I  0   0.0  Python.exe

4 jobs; 4 idle, 0 running, 0 held
"""
        condor.queue = fake_queue(0, queue_output)
        self.assertEqual([('xs205803.MELINDA.LOCAL', '200.0'),
                          ('xs205803.MELINDA.LOCAL', '201.0'),
                          ('xs205803.MELINDA.LOCAL', '202.0'),
                          ('xs205804.MELINDA.LOCAL', '200.0')],
                         condor.job_ids(None))

    def test_job_ids_empty(self):
        queue_output = """All queues are empty """
        condor.queue = fake_queue(0, queue_output)
        self.assertEqual(condor.job_ids(None), [])

    def test_job_ids_error(self):
        condor.queue = fake_queue(-1, 'No such file or directory')
        self.assertEqual(condor.job_ids(None), [])

    def test_buildenv(self):
        self.assertEqual('"babar=\'ba ""ba"" r\' TARGET=42"',
                         condor._build_environment({'TARGET': '42',
                                                    'babar': 'ba "ba" r'}))

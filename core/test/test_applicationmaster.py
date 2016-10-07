import sys
import os
from core.applicationmaster import ApplicationMaster
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../")


class Test:
    def setup(self):
        self.am = ApplicationMaster(host="hdfs10.internal.shutterfly.com")

    def teardown(self):
        pass

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def test_historyserver_info(self):
        assert self.am.applicationmaster_info()

    def test_mapreduce_jobs(self):
        assert self.am.mapreduce_jobs(limit=1, state='SUCCEEDED', user='firehose', finished_time_end=1468091434057)

    def test_mapreduce_job(self):
        assert self.am.mapreduce_job(job_id='job_1467952761993_0359')

    def test_mapreduce_job_attempts(self):
        assert self.am.mapreduce_job_attempts(job_id='job_1467952761993_0359')

    def test_mapreduce_job_counters(self):
        assert self.am.mapreduce_job_counters(job_id='job_1467952761993_0359')

    def test_mapreduce_job_conf(self):
        assert self.am.mapreduce_job_conf(job_id='job_1467952761993_0359')

    def test_mapreduce_job_tasks(self):
        assert self.am.mapreduce_job_tasks(job_id='job_1467952761993_0359')

    def test_mapreduce_job_task(self):
        assert self.am.mapreduce_job_task(job_id='job_1467952761993_0359', task_id='task_1467952761993_0359_m_000000')

    def test_mapreduce_job_task_counters(self):
        assert self.am.mapreduce_job_task_counters(job_id='job_1467952761993_0359',
                                                   task_id='task_1467952761993_0359_m_000000')

    def test_mapreduce_job_task_attempts(self):
        assert self.am.mapreduce_job_task_attempts(job_id='job_1467952761993_0359',
                                                   task_id='task_1467952761993_0359_m_000000')

    def test_mapreduce_job_task_attempt(self):
        assert self.am.mapreduce_job_task_attempt(job_id='job_1467952761993_0359',
                                                  task_id='task_1467952761993_0359_m_000000',
                                                  attempt_id='attempt_1467952761993_0359_m_000000_0')

    def test_mapreduce_job_task_attempt_counters(self):
        assert self.am.mapreduce_job_task_attempt_counters(job_id='job_1467952761993_0359',
                                                           task_id='task_1467952761993_0359_m_000000',
                                                           attempt_id='attempt_1467952761993_0359_m_000000_0')
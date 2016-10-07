import sys
import os
from core.resourcemanager import ResourceManager
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../")


class Test:
    def setup(self):
        self.rm = ResourceManager(host="hdfs11.internal.shutterfly.com")

    def teardown(self):
        pass

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def test_cluster_info(self):
        assert self.rm.cluster_info()

    def test_cluster_metrics(self):
        assert self.rm.cluster_metrics()

    def test_cluster_scheduler(self):
        assert self.rm.cluster_scheduler()

    def test_cluster_applications(self):
        assert self.rm.cluster_applications()

    def test_cluster_applications_with_app_id(self):
        assert self.rm.cluster_applications(app_id='application_1468263334028_0051')

    def test_cluster_appstatistics(self):
        assert self.rm.cluster_appstatistics()

    def test_cluster_appattempts(self):
        assert self.rm.cluster_appattempts(app_id='application_1468263334028_0051')

    def test_cluster_nodes(self):
        assert self.rm.cluster_nodes()

    def test_cluster_node(self):
        assert self.rm.cluster_node(node_id='hdfs13.internal.shutterfly.com')

    def test_cluster_appstate(self):
        assert self.rm.cluster_appstate(app_id='application_1468263334028_0051')

    def test_cluster_appqueue(self):
        assert self.rm.cluster_appqueue(app_id='application_1468263334028_0051')
import sys
import os
from core.nodemanager import NodeManager
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../")


class Test:
    def setup(self):
        self.nm = NodeManager(host="hdfs22.internal.shutterfly.com")

    def teardown(self):
        pass

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def test_node_info(self):
        assert self.nm.node_info()

    def test_node_apps(self):
        assert self.nm.node_apps()

    def test_node_app(self):
        assert self.nm.node_app(app_id='application_1468280338115_1170')

    def test_node_containers(self):
        assert self.nm.node_containers()

    def test_node_container(self):
        assert self.nm.node_container(container_id='container_1468280338115_1171_01_000001')
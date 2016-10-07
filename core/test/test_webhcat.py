import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../core")


from webhcat import Webhcat

import getpass
import test_config as tconfig

class Test:
    def setup(self):
        pass

    def teardown(self):
        pass

    @classmethod
    def setup_class(cls):
        webhcat_host = tconfig.webhcat["host"] or "localhost"
        webhcat_port = tconfig.webhcat["port"] or 50111
        cls.user = getpass.getuser()
        cls.whcat = Webhcat(host=webhcat_host, port=webhcat_port, user=cls.user)
        pidname = str(os.getpid())
        cls.database = "default"
        cls.table = "test_" + pidname

    @classmethod
    def teardown_class(cls):
        try:
            cls.whcat.drop_table(database=cls.database, table=cls.table)
        except:
            pass

    def test_001_get_db_metadata(self):
        dblist = self.whcat.get_databases()
        assert len(dblist) != 0

    def test_002_create_table(self):
        comment = "test table"
        columns = [{"name": "id", "type": "bigint"},
                   {"name": "price", "type": "float", "comment": "The unit price"}]
        database = "default"
        partition_by = [{"name": "country", "type": "string"}]
        format = {"storedAs": "rcfile"}

        self.whcat.create_table(table=self.table, columns=columns, database=self.database,
                     comment=comment, partition_by=partition_by, cluster_by=None,
                     format=format)
        assert len(self.whcat.get_table_metadata(database=database, table=self.table)) > 0

    def test_003_add_partition(self):
        location = "/tmp/country/algeria"
        partition = "country='algeria'"
        self.whcat.add_partition(self.table, partition,
                          location, database=self.database)
        assert self.whcat.get_partition(self.table, partition, database=self.database)

    def test_004_rename_table(self):
        rename = self.table + "_rename"
        assert self.whcat.rename_table(table=self.table, database=self.database, rename=rename)
        try:
            self.whcat.get_table_metadata(table=self.table)
            raise AssertionError("rename unsuccessful")
        except IOError:
            pass
        assert self.whcat.get_table_metadata(table=rename)
        assert self.whcat.rename_table(table=rename, database=self.database, rename=self.table)

    def test_005_copy_table(self):
        new_table = self.table + "_copy"
        assert self.whcat.copy_table(table=self.table, database=self.database, new_table=new_table)
        assert self.whcat.get_table_metadata(table=new_table)
        self.whcat.drop_table(database=self.database, table=new_table)

    def test_006_drop_table(self):
        assert self.whcat.drop_table(database=self.database, table=self.table)
        try:
            self.whcat.get_table_metadata(table=self.table)
            raise AssertionError("rename unsuccessful")
        except IOError:
            pass

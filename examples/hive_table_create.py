import sys
import os
# use PYTHONPATH to setup path
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../core")
from webhcat import Webhcat
import config as tconfig


def main():
    web_hcat_port = tconfig.webhcat["port"] or 50111
    web_hcat_host = tconfig.webhcat["host"] or "localhost"
    table="test2"
    whcat = Webhcat(host=web_hcat_host, port=web_hcat_port,
                    default_database="default", user="hive")
    comment = "test table"
    columns = [{"name": "id", "type": "bigint"},
               {"name": "price", "type": "float", "comment": "The unit price" }]
    database = "default"
    partition_by = [{"name": "country", "type": "string"}]
    format = {"storedAs": "rcfile"}
    try:
        whcat.drop_table(database=database, table=table)
    except:
        pass
    #whcat.drop_table(database=database, table=table)
    whcat.create_table(table=table, columns=columns, database=database,
                       comment=comment, partition_by=partition_by, cluster_by=None,
                       format=format)
    location = "/tmp/country/algeria"
    partition = "country='algeria'"
    whcat.add_partition(table, partition,
                        location, database=database)
    print(whcat.get_partition(table, partition, database=database))

if __name__ == "__main__":
    main()
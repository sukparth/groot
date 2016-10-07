import sys
import os
# use PYTHONPATH to setup path
import config as tconfig

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../core")
from webhcat import Webhcat


def main():
    web_hcat_port = tconfig.webhcat["port"] or 50111
    web_hcat_host = tconfig.webhcat["host"] or "localhost"
    whcat = Webhcat(host=web_hcat_host, port=web_hcat_port,
                    default_database="default")
    #print whcat.get_databases()
    for table in whcat.get_tables("default"):
        try:
            metadata = whcat.get_table_metadata("default",table)
            print metadata
        except:
            pass
        

if __name__ == "__main__":
    main()
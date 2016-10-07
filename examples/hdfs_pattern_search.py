import sys
import os
# use PYTHONPATH to setup path
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../core")
from core.webhdfs import Webhdfs, WhdfsSearchExpression, WhdfsSearchExpressionList
import datetime
import time
from pprint import pprint
from datetime import datetime
import config as tconfig

def advanced_search_print(whdfs, path):
    dt = datetime.datetime(2013, 2, 25, 23, 23, 23)
    unix_time = long(time.mktime(dt.timetuple()) * 1000) # Convert to milliseconds
    selist = WhdfsSearchExpressionList()
    selist.add(WhdfsSearchExpression("mtime", ">", long(unix_time)))
    selist.add(WhdfsSearchExpression("owner", "glob", "rprasad"))
    selist.add("or")
    for root, dirs, files in whdfs.scan_dir(path, search_exp_list = selist,
                                            ignore_error = False):
        print root,dirs,files
    
def main():
    web_hdfs_host = tconfig.webhdfs["host"] or "localhost"
    web_hdfs_port = tconfig.webhdfs["port"] or 50070
    path = "/apps/hive/warehouse/rprasad.db"
    pattern="^promo.*log"
    pattern_type = "regex"
    pattern = None
    #pattern="^promo.*log"
    whdfs = Webhdfs(host=web_hdfs_host, port=web_hdfs_port, user="hdfs")
    path = "/apps/hive/warehouse/rprasad.db"
    pprint(whdfs.long_list_dir( path=path, otype="all",
                        ignore_error=False,
                        ext_status=True ))
    #advanced_search_print(whdfs, path)

if __name__ == "__main__":
    main()
import sys
import os
# use PYTHONPATH to setup path
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../core")
import config as tconfig
from webhdfs import Webhdfs
from util import timer

@timer
def main():
    web_hdfs_host = tconfig.webhdfs["host"] or "localhost"
    web_hdfs_port = tconfig.webhdfs["port"] or 50070
    path = "/dwh/data/dwcore/dwhstatsevent/statisticsevent"
    whdfs = Webhdfs(host=web_hdfs_host, port=web_hdfs_port, user="hdfs")
    for root, dirs, files in whdfs.scan_dir(path):
        dir_util = whdfs.get_content_summary(root)['length']
        print("{0} has size {1} and object count {2}"
               .format(root,
                       dir_util,
                       len(dirs)+len(files)))

if __name__ == "__main__":
    main()

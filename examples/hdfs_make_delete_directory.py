import sys
import os
# use PYTHONPATH to setup path
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../core")
from webhdfs import Webhdfs
from util import timer
import config as tconfig

@timer
def main():
    web_hdfs_host = tconfig.webhdfs["host"] or "localhost"
    web_hdfs_port = tconfig.webhdfs["port"] or 50070
    path = "/tmp/testdir"
    whdfs = Webhdfs(host=web_hdfs_host, port=web_hdfs_port)
    whdfs.delete(path)
    whdfs.make_dir(path)
    whdfs.get_path_status(path)
    whdfs.change_owner(path, "psukumar")
    whdfs.delete(path)

if __name__ == "__main__":
    main()

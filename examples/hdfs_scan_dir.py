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
    path = "/apps"
    skip_dirs = ["/apps/hive/","/apps/hbase/"]
    whdfs = Webhdfs(host=web_hdfs_host, port=web_hdfs_port)
    for root, dirs, files in whdfs.scan_dir(path, skip_dirs=skip_dirs):
        for dir in dirs:
            dir['fullPath'] = root + "/" + dir['pathSuffix']
            print(dir)
        for file in files:
            file['fullPath'] = root + "/" + file['pathSuffix']
            print(file)

if __name__ == "__main__":
    main()

import sys
import os
import config as tconfig

sys.path.append(os.path.dirname( os.path.realpath( __file__ ) ) + "/../core")
from webhdfs import Webhdfs
from pprint import pprint


def main():
    web_hdfs_host = tconfig.webhdfs["host"] or "localhost"
    web_hdfs_port = tconfig.webhdfs["port"] or 50070
    srcfile = "C:/Users/psukumar/desktop/browser_type.tsv"
    tgtpath = "/user/psukumar/browser_type.tsv"
    whdfs = Webhdfs(host=web_hdfs_host, port=web_hdfs_port)
    block_size = 32000000
    buffer_size = 32000
    whdfs.upload_file(srcfile, tgtpath, block_size=block_size,
                           buffer_size=buffer_size, overwrite=True)

    pprint(whdfs.get_path_status(tgtpath))



if __name__ == "__main__":
    main()
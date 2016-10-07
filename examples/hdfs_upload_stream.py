import sys
import os
import config as tconfig

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../core")
from webhdfs import Webhdfs
from pprint import pprint


def gen_data(val=100000):
    for i in range(1,val):
        yield str(i) + "\n"
    raise StopIteration()

def main():
    web_hdfs_host = tconfig.webhdfs["host"] or "localhost"
    web_hdfs_port = tconfig.webhdfs["port"] or 50070
    read_desc, write_desc = os.pipe()
    tgtfile = "/user/psukumar/gentest2.csv"
    whdfs = Webhdfs(host=web_hdfs_host, port=web_hdfs_port)
    block_size = 32000000
    buffer_size = 32000
    whdfs.upload_data_iter(gen_data(), tgtfile, block_size=block_size,
                       buffer_size=buffer_size, overwrite=True)
    whdfs.append_data_iter(gen_data(), tgtfile, buffer_size=buffer_size)
    pprint(whdfs.get_path_status(tgtfile))

if __name__ == "__main__":
    main()
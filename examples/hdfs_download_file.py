import sys
import os
import config as tconfig

sys.path.append( os.path.dirname( os.path.realpath( __file__ ) ) + "/../core" )
from webhdfs import Webhdfs
from pprint import pprint

def main():
    web_hdfs_host = tconfig.webhdfs["host"] or "localhost"
    web_hdfs_port = tconfig.webhdfs["port"] or 50070
    tgtpath = "C:/Users/psukumar/desktop"
    path = "/user/psukumar"
    pattern = "Shutterfly_omniture_session_feed_201407*.zip"
    whdfs = Webhdfs( host=web_hdfs_host, port=web_hdfs_port )
    #print whdfs.list_dir( path=path, otype="file" )
    filelist = [os.path.join( path, filename ) for filename in \
                whdfs.list_dir( path=path, otype="all", \
                                pattern=pattern )]
    filename = "browser_type.tsv"
    filepath = path + "/" + filename
    filelist = [filepath]
    #print whdfs.list_dir( path, otype="file" )
    pprint(whdfs.long_list_dir( filepath, otype="all",
                          ignore_error=False ))
    whdfs.download_file( filepath, tgtpath, overwrite=True)
    whdfs.download_files( filelist, tgtpath, overwrite=False )


if __name__ == "__main__":
    main( )
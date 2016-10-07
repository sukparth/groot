import sys
import os
import test_config as tconfig

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../")


from core.webhdfs import Webhdfs

import getpass

class Test:
    def setup(self):
        pass

    def teardown(self):
        pass


    @classmethod
    def setup_class(cls):
        web_hdfs_host = tconfig.webhdfs["host"] or "localhost"
        web_hdfs_port = tconfig.webhdfs["port"] or 50070
        cls.user =  getpass.getuser()
        cls.whdfs = Webhdfs(host=web_hdfs_host, port=web_hdfs_port, user=cls.user)
        pidname = str(os.getpid())

        # Local dir/file setup
        if os.name == "nt":
            cls.local_dir = "C:\\Users\\" + cls.user + "\\AppData\\Local\\Temp"
        else:
            cls.local_dir = "/tmp"
        cls.local_rand_file_1 = os.path.join(cls.local_dir, pidname + "_" + cls.user + "_1.dat")
        cls.local_rand_file_2 = os.path.join(cls.local_dir, pidname + "_" + cls.user + "_2.dat")

        # HDFS setup
        cls.hdfs_dir = "/tmp"
        cls.hdfs_webhcat = "/apps/webhcat"
        cls.hdfs_apps = "/apps"

        with open(cls.local_rand_file_1, 'wb') as fout:
            fout.write(os.urandom(1024))
        with open(cls.local_rand_file_2, 'wb') as fout:
            fout.write(os.urandom(1024))

        cls.hdfs_rand_parent_dir = cls.hdfs_dir + "/" + cls.whdfs.user + "_" + pidname
        cls.hdfs_rand_child_dir = cls.hdfs_rand_parent_dir + "/" + pidname
        cls.hdfs_rand_child_dir_2 = cls.hdfs_rand_parent_dir + "/" + pidname + "_2"
        cls.hdfs_rand_file_1 = cls.hdfs_rand_parent_dir + "/" + pidname + "_" + cls.whdfs.user + "_1.dat"
        cls.hdfs_rand_file_2 = cls.hdfs_rand_parent_dir + "/" + pidname + "_" + cls.whdfs.user + "_2.dat"
        cls.hdfs_rand_file_3 = cls.hdfs_rand_parent_dir + "/" + pidname + "_" + cls.whdfs.user + "_3.dat"
    @classmethod
    def teardown_class(cls):
        try:
            os.remove(cls.local_rand_file_1)
            os.remove(cls.local_rand_file_2)
            cls.whdfs.delete(cls.hdfs_rand_file_1)
            cls.whdfs.delete(cls.hdfs_rand_file_2)
            cls.whdfs.delete(cls.hdfs_rand_parent_dir, recursive=True)
        except IOError:
            pass
 
    def test_001_filesystem_object_exists(self):
        assert self.whdfs.is_exists(self.hdfs_dir)

    def test_002_filesystem_object_not_exists(self):
        assert not self.whdfs.is_exists(self.hdfs_rand_parent_dir)

    def test_003_is_directory(self):
        assert self.whdfs.is_dir(self.hdfs_dir), \
            "{0} is not a directory".format(self.hdfs_dir)

    def test_004_list_dir(self):
        assert len(self.whdfs.list_dir(self.hdfs_apps)) != 0

    def test_005_create_dir(self):
        self.whdfs.make_dir(self.hdfs_rand_parent_dir)

    def test_006_create_dir_fail_if_already_exists(self):
        user_dir=self.hdfs_rand_parent_dir
        try:
            self.whdfs.make_dir(self.hdfs_rand_parent_dir)
            raise AssertionError("test_create_dir_already_exists " +
                                 "failed for {0}".format(user_dir))
        except IOError:
            pass

    def test_007_create_dir_if_not_parent(self):
        # Use whdfs.make_dirs
        self.whdfs.make_dirs(self.hdfs_rand_child_dir)
        assert self.whdfs.is_dir(self.hdfs_rand_child_dir), \
            "{0} does not exist".format(self.hdfs_rand_child_dir)

    
    def test_008_scan_dir(self):
        path = self.hdfs_webhcat
        gen = self.whdfs.scan_dir(path)
        print(self.whdfs.list_dir(path=path, otype="file"))
        assert next(gen)
        del gen

    def test_008_scan_dir_1(self):
        path = self.hdfs_webhcat
        gen = self.whdfs.scan_dir( path, skip_dirs=self.hdfs_rand_child_dir_2 )
        print(self.whdfs.list_dir(path=path, otype="dir"))
        assert next(gen)
        del gen

    def test_009_upload_file(self):
        # Try uploading a file to local
        assert not self.whdfs.is_exists(self.hdfs_rand_file_1)
        self.whdfs.upload_file(self.local_rand_file_1, self.hdfs_rand_file_1)
        assert self.whdfs.is_exists(self.hdfs_rand_file_1)

    def test_010_upload_files(self):
        # Try uploading files to local
        assert not self.whdfs.is_exists(self.hdfs_rand_file_2)
        infilelist = [self.local_rand_file_1, self.local_rand_file_2]
        self.whdfs.upload_files(infilelist, self.hdfs_rand_parent_dir)
        assert self.whdfs.is_file(self.hdfs_rand_file_1) and self.whdfs.is_file(self.hdfs_rand_file_2)

    def test_012_append_file(self):
        # Try uploading files to local
        infile = self.local_rand_file_1
        srcfilesize = os.path.getsize(infile)
        tgtfilesize = self.whdfs.get_path_status(self.hdfs_rand_file_1)["length"]
        self.whdfs.append_file(infile, self.hdfs_rand_file_1)
        newtgtfilesize = self.whdfs.get_path_status( self.hdfs_rand_file_1 )["length"]
        assert newtgtfilesize - tgtfilesize == srcfilesize

    def test_013_download_file(self):
        # Try downloading a file to local
        os.remove(self.local_rand_file_1)
        assert not os.path.isfile(self.local_rand_file_1)
        self.whdfs.download_file(self.hdfs_rand_file_1, self.local_rand_file_1)
        assert os.path.isfile(self.local_rand_file_1)
        # self.local_rand_file_1

    def test_014_download_files(self):
        # Try downloading files to local
        os.remove(self.local_rand_file_1)
        os.remove(self.local_rand_file_2)
        assert not os.path.isfile(self.local_rand_file_1) and  not os.path.isfile(self.local_rand_file_2)
        hdfs_file_list = [self.hdfs_rand_file_1, self.hdfs_rand_file_2]
        self.whdfs.download_files(hdfs_file_list, self.local_dir)
        assert os.path.isfile(self.local_rand_file_1) and os.path.isfile(self.local_rand_file_2)

    def test_016_rename(self):
        # Use whdfs.rename
        # renames path
        #newpath=
        #self.whdfs.rename(self.hdfs_rand_child_dir)
        #assert not self.whdfs.is_exists(self.hdfs_rand_child_dir)
        pass

    def test_016_create_symlink(self):
        # Use whdfs.create_symlink
        # Creates symlink
        pass

    def test_017_concat_files(self):
        # Concatenate files
        tgtfilesize1 = self.whdfs.get_path_status(self.hdfs_rand_file_1)["length"]
        tgtfilesize2 = self.whdfs.get_path_status( self.hdfs_rand_file_2 )["length"]
        self.whdfs.concat_files(self.hdfs_rand_file_1, self.hdfs_rand_file_2)
        newtgtfilesize = self.whdfs.get_path_status(self.hdfs_rand_file_1)["length"]
        assert newtgtfilesize == tgtfilesize1 + tgtfilesize2

    def test_040_delete(self):
        # Use whdfs.delete
        # Deletes path
        self.whdfs.delete(self.hdfs_rand_child_dir)
        assert not self.whdfs.is_exists(self.hdfs_rand_child_dir)
        self.whdfs.make_dirs(self.hdfs_rand_child_dir)

         
        
         
    

        

        
        
    
 
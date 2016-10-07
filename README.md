# GROOT

## Synopsis
Groot is a python library that provides a python based client to access HDFS, HCATALOG and YARN services using rest api's for communicating with the services.
It also integrates search functionality for certain methods such as scanning HDFS namespace. This is had been tested with HDP 2.2+. Currently only works with non kerberos or SPNEGO based http connections to the cluster but work is in progress to add enhanced authorization methods.

## Pre-requisites
    sudo pip install requests
    sudo pip install six
    sudo pip install nose

## Environment
	os: Windows, Linus and OSX
    python : 2.6+ & 3+
    Hadoop : 2.7+ (can work with earlier versions but certains 
				   functions will error out)

## Setup
    cd <install dir>
    git clone <ssh or https url>

    Windows
    SET PYTHONPATH=%PYTHONPATH%;<install dir>/groot/core

    Unix
    export PYTHONPATH=$PYTHONPATH:<install dir>/groot/core

## Usage

### Hadoop File System Operations

 - [Initialize Hdfs connection](#initialize-hdfs-connection)
 - [List a directory](#list-a-directory)
 - [Long list a directory](#long-list-a-directory)
 - [Make a directory](#make-a-directory)
 - [Rename a file/directory](#rename-a-filedirectory)
 - [Delete a file/directory](#delete-a-filedirectory)
 - [Create a symbolic link](#create-a-symbolic-link)
 - [Status of a file/directory](#status-of-a-filedirectory)
 - [Scan a directory](#scan-a-directory)
 - [Get File Iterator](#get-file-iterator)
 - [Print a file](#print-a-file)
 - [Download a file](#download-a-file)
 - [Download files](#download-files)
 - [Upload a file](#upload-a-file)
 - [Upload files](#upload-files)
 - [Upload a directory](#upload-directory)
 - [Upload Data Iterator](#upload-data-iterator)
 - [Append a file](#append-a-file)
 - [Append Data Iterator](#append-data-iterator)
 - [Concatenate files](#concatenate-files)

### Hive/HCat Operations

 - [Initialize Hcat connection](#initialize-hcat-connection)
 - [Get databases](#get-database-list)
 - [Get database metadata](#get-database-metadata)
 - [Get tables](#get-tables)
 - [Create table](#create-table)
 - [Rename table](#rename-table)
 - [Drop table](#drop-table)
 - [Copy table](#copy-table)
 - [Add partition](#add-partition)
 - [Get partition](#get-partition)

### Extended Examples
	
 - [Get table list and metadata](#get-table-list-and-metadata)
 - [Scan a directory for large files](#scan-a-directory-for-large-files)
 - [Print disk utilization for objects under a path](#print-disk-utilization-for-objects-under-a-path)

#### Initialize Hdfs connection
	Webhdfs(host=None, port=50070, url=None, protocol="http",user=None)

      Args:
          host(str)     : The host where webhdfs service is running.
            or list)      This is a required argument. This can be a 
				          single host, a string containing hosts in a 
				          HA separated by comma(,) or a list of hosts 
				          in a HA setup.The connection attempt will be 
				          made in order from left to right.
          port(int)     : The port on which webhdfs is listening.
          url(str)      : Url which is a combination of protocol, host 
				          and port
          protocol(str) : The  protocol to be used to connect to
			              webhdfs. The default is 'http'
          user(str)     : Defaults to the client os user. 
          
      Example:
	    from webhdfs import Webhdfs
	    whdfs = Webhdfs("namenode_primary.host.com, 
						 namenode_secondary.host.com", 50070)


#### List a directory

    list_dir(path, otype, pattern, pattern_type)
    
    	List contents of directory
        Args: 
         path(str)         : Full path to start the recursive listing 
         otype(str)        : Object type 
                             otype = "all" will list all contents 
                             otype = "file" will list only files 
                             otype = "dir" will list only dirs 
         pattern(str)      : Search for files/directories of specific 
					         pattern 
                             based on pattern_type . 
         pattern_type(str) : "glob" or "regex". Default glob. 
         ext_status(bool)  : True means get extended statistics. 
					         Default False.
       Returns:
        List of files/directories names under path.
        
       Example:
	    from webhdfs import Webhdfs
	    
	    whdfs = Webhdfs("namenode.host.com", 50070)
	    whdfs.list_dir("apps")

#### Long list a directory
    long_list_dir(path, otype, pattern, pattern_type, ignore_error,
                  ext_status)
                  
    List contents of directory    
    Args:
        path(str)          : Full path to start the recursive 
              listing
        otype(str)         : Object type
                             otype = "all" will list all contents
                             otype = "file" will list only files
                             otype = "dir" will list only dirs
        pattern(str)       : Search for files of specific pattern
                             based on pattern_type .
        pattern_type(str)  : "glob" or "regex". Default glob.
        ignore_error(bool) : Ignore any errors during search such as 
					         access issues.
        ext_status(bool)   : Produces extra status information namely 
						     content summary for directories.
    Returns:
    List of dictionaries containing file/directory status information.
    {
          u'group': u'XXX', u'permission': u'777',
          u'blockSize': N, u'accessTime': N, u'pathSuffix': u'XYZ',
          u'modificationTime': NNNNNNNL, u'replication': N,
          u'length': N, u'childrenNum': N, u'owner': u'user',
          u'storagePolicy': N, u'type': u<'DIRECTORY' or 'FILE'>,
          u'fileId': NNNNNNN
     }
     
     Example:
	   from webhdfs import Webhdfs
	   
	   whdfs = Webhdfs("namenode.host.com", 50070)
	   whdfs.long_list_dir("apps")

#### Make a directory
    make_dir(path, permission)
    
    Create a directory in safe mode.
    If the directory exists or the parent directory does not exist
    it will throw an exception.

        Args:
            path(str)         : Directory path to create
            permission(int)   : Octal permissions
     Example:
	   from webhdfs import Webhdfs
	   
	   whdfs = Webhdfs("namenode.host.com", 50070)
	   whdfs.make_dir("/tmp/xdir",755)  

#### Rename a file/directory
	rename(self, path, newpath)
	
	Rename a file or directory
	Args:
	   path(str)         : Path on HDFS to rename
	   newpath(str)      : Renamed Path
	
	Example:
	   from webhdfs import Webhdfs
	   
	   whdfs = Webhdfs("namenode.host.com", 50070)
	   whdfs.rename("/tmp/xfile","/tmp/yfile") 

#### Delete a file/directory
     delete(path, recursive)
     
     Delete a path.
     Args:
         path(str)         : Path on HDFS to rename
         recursive(Bool)   : If True will recursively delete. 
					         Default is False. 
	Example:
	   from webhdfs import Webhdfs
	   
	   whdfs = Webhdfs("namenode.host.com", 50070)
	   whdfs.delete("/tmp/xfile")  
	   whdfs.delete("/tmp/xdir", recursive = True) 	


#### Create a symbolic link
	 create_symlink(path, destination, create_parent=False):

     Creates a symbolic link to destination.
     Args:
         path(str)          : Symlink path
         destination(str)   : target path
         create_parent(str) : Creates parent directory for 
							  symlink. Default value is False.
	  Example:
	   from webhdfs import Webhdfs
	   
	   whdfs = Webhdfs("namenode.host.com", 50070)
	   whdfs.create_symlink("/tmp/xlink","/tmp/xfile")  

#### Status of a file/directory
	 get_path_status(self, path, ignore_error=False)
	 
     Get path status information
     Args:
         path                 : Path
         ignore_error(bool)   : ignore error
     Returns:
         Path status of file system object represented by a  
         dictionary of format
         {
               u'group': u'XXX', u'permission': u'NNN',
               u'blockSize': N, u'accessTime': N, 
               u'pathSuffix': u'XYZ', 
               u'modificationTime': NNNNNNNL, u'replication': N,
               u'length': N, u'childrenNum': N, u'owner': u'user',
               u'storagePolicy': N, 
               u'type': u<'DIRECTORY', 'FILE' or 'SYMLINK'>,
               u'fileId': NNNNNNN
          } 
          
      Example:
		  from webhdfs import Webhdfs 
		  whdfs = Webhdfs("namenode.host.com", 50070)
		  whdfs.get_path_status("/tmp/xfile") 

#### Scan a directory
     scan_dir(self, path, level=None, pattern=None, 
		      pattern_type="glob", otype="all",
		      ignore_error=True, skip_dir=None,
		      search_exp_list=None)

     Walk through the filesystem starting from path with long listing 
     for each filesystem object.

        Args:
            path(str)           : Full path to start the recursive 
						          listing
            level(int)          : Maximum depth of directories from the 
						          path to list
            pattern(str)        : Search for files of specific pattern
                                  based on pattern_type .
            pattern_type(str)   : "glob" or "regex". Default glob.
            otype(str)          : File, dir or all. Default all
            ignore_error(bool)  : Default set to True. If set to false 
						          when it encounters a object or 
						          directory it cannot access anymore it 
						          will fail.
            skip_dirs           : List of directory to skip recursing 
						          into.
            search_exp_list(WhdfsSearchExpressionList)
					            : Search expression list to search 
					              elements of path status.

        Returns:
            Generator returning path, dirlist, filelist where dirlist 
            and filelist contains status of directories and files under
            path respectively.

        Example:
        # Scan directories printing out parent, child directories and 
          files
            from webhdfs import Webhdfs
            
            whdfs = Webhdfs(host=localhost, port=50070)
            for root,dirs,files in whdfs.scan_dir("/user/me", otype = 
	            "all", level = 0):
                print root
                for dir in dirs['pathSuffix']:
                    print dir
                for file in files['pathSuffix']:
                    print file

#### Download a file
     download_file(self, srcfile, tgtpath, overwrite=True)
    
     Downloads hdfs file to local directory

	    Arguments:
	    srcfile(str)       : hdfs file to be downloaded
	    tgtpath(str)       : The target directory
        overwrite(bool)    : When overwrite is set to True it will 
					         replace target file if it exists.

		Example:
		from webhdfs import Webhdfs
		
        whdfs = Webhdfs(host=localhost, port=50070)
	    filepath = "/user/me/xfile"
	    tgtpath = "/home/me"
	    whdfs.download_file( filepath, tgtpath, overwrite=True)

#### Get File Iterator
     iter_file(self, srcfile, length=None, buffer_size=None)
     
     Returns an read iterator over hdfs file
        Args:
            srcfile(str)       : hdfs file to be print
            length(int)        : length to read
            buffer_size         : buffer size for each read
            
		Example:
		from webhdfs import Webhdfs
		# Print top 10 rows in a hdfs file
        whdfs = Webhdfs(host=localhost, port=50070)
	    filepath = "/user/me/xfile"
	    count = 10
	    counter = 0
        for block in self.iter_file(srcfile=srcfile,
                               length=length,
                               buffer_size=1000):
            if block: # filter out keep-alive new chunks
	            blocklist = str(block).split("\n")
	            counter += len(blocklist)
	            if counter >= count:
		            print("\n".join(blocklist[:count+1])
		         else:
			        print("\n".join(blocklist))
			        
#### Print a file
     print_file(self, srcfile, tgtpath, overwrite=True)
    
	 Print contents of hdfs file
        Args:
            srcfile(str)       : hdfs file to be print
            length(int)        : length to read
            buffer_size         : buffer size for read

		Example:
		from webhdfs import Webhdfs
		
        whdfs = Webhdfs(host=localhost, port=50070)
	    filepath = "/user/me/xfile"
	    whdfs.print_file(filepath)

#### Download files
     download_files(srcfilelist, tgtpath, overwrite=True, 
					   ignore_error=True)
    
     Downloads hdfs files to local directory

	    Arguments:
	    srcfilelist(list)  : list of hdfs files to be downloaded
	    tgtpath(str)       : The target directory
        overwrite(bool)    : When overwrite is set to True it will 
					         replace target file if it exists.
        ignore_error(bool) : if set to True will ignore download 
						     failures.
						     
		Example:
		from webhdfs import Webhdfs
		
        whdfs = Webhdfs(host=localhost, port=50070)
	    filepathlist = ["/user/me/xfile", "/user/me/yfile"]
	    tgtpath = "/home/me"
	    whdfs.download_files(filepathlist, tgtpath, 
								 overwrite=True)

#### Upload a file
     upload_file(srcfile, tgtpath, block_size=None, replication=None,
                    permission=None, buffer_size=None, 
                    create_parent=False,
                    overwrite=True)
                    
	 Uploads local files to hdfs directory

        Args:
            srcfile(str)       : local source file to be uploaded.
            tgtpath(str)       : The target hdfs directory or file.
            block_size(int)    : Size of block for the hdfs file.
            replication(int)   : Replication factor for the file.
            permission(octal)  : File permissions to be set. 
							         Example 755.
            buffer_size(int)   : Size of buffer used for data transfer.
            overwrite(bool)    : True will overwrite the file if 
						             exists. Default True.
            create_parent(bool): Creates parent folder before upload
                                    if set to True. Default is  False.

		Example:
		from webhdfs import Webhdfs
		
        whdfs = Webhdfs(host=localhost, port=50070)
	    block_size = 128000000
	    buffer_size = 32000
	    srcfile = "/home/me/x.txt"
	    tgtpath = "/user/me"
	    whdfs.upload_file(srcfile, tgtpath, block_size=block_size,
                           buffer_size=buffer_size, overwrite=True)

#### Upload data iterator
     upload_data_iter(data_iter, tgtfile, block_size=None,
			     replication=None,
                 permission=None, buffer_size=None, 
                 create_parent=False,
                 overwrite=True)
                    
	 Uploads data produced by an iterator to hdfs directory

        Args:
            data_iter(iter)    : Iterator such as generator function.
            tgtfile(str)       : The target hdfs file.
            block_size(int)    : Size of block for the hdfs file.
            replication(int)   : Replication factor for the file.
            permission(octal)  : File permissions to be set. 
							         Example 755.
            buffer_size(int)   : Size of buffer used for data transfer.
            overwrite(bool)    : True will overwrite the file if 
						             exists. Default True.
            create_parent(bool): Creates parent folder before upload
                                    if set to True. Default is  False

      Example:
		from webhdfs import Webhdfs
		
        def gen_data(val=100000):
		    for i in range(1,val):
		        yield str(i) + "\n"
		    raise StopIteration()
		 
	    whdfs = Webhdfs(host=localhost, port=50070)
	    block_size = 128000000
	    buffer_size = 32000
	    srcfile = "/home/me/x.txt"
	    tgtpath = "/user/me"
	    whdfs.upload_data_ter(gen_data(), tgtfile, 
							  block_size=block_size,
	                          buffer_size=buffer_size, overwrite=True)
                           
#### Upload files
     upload_files(srcfilelist, tgtpath, block_size=None, 
			    replication=None, permission=None,
			    buffer_size=None, create_parent=False,
                overwrite=True, ignore_error=False)
                    
	 Uploads local files to hdfs directory

        Args:
            srcfilelist(str)   : local source file to be uploaded.
            tgtpath(str)       : The target hdfs directory.
            block_size(int)    : Size of block for the hdfs file.
            replication(int)   : Replication factor for the file.
            permission(octal)  : File permissions to be set. 
							         Example 755.
            buffer_size(int)   : Size of buffer used for data transfer.
            overwrite(bool)    : True will overwrite the file if 
						             exists. Default True.
            ignore_error(bool) : If set to True will ignore failures
                                    and not raise exception.
            create_parent(bool): Creates parent folder before upload
                                    if set to True. Default is  False

		Example:
		from webhdfs import Webhdfs
		
        whdfs = Webhdfs(host=localhost, port=50070)
	    block_size = 128000000
	    buffer_size = 32000
	    srcfilelist = ["/home/me/x.txt","/home/me/y.txt"]
	    tgtpath = "/user/me"
	    whdfs.upload_files(srcfilelist, tgtpath, block_size=block_size,
                           buffer_size=buffer_size, overwrite=True)

#### Upload Directory
        upload_dir(srcpath, tgtpath, block_size=None, replication=None,
                   permission=None, buffer_size=None, create_parent=False,
                   overwrite=True, ignore_error=False)

        Uploads a local directory to HFDS
        Args:
            srcpath          : local directory to be uploaded.
            tgtpath         : The target hdfs directory.
            block_size      : Size of block for the hdfs file.
            replication     : Replication factor for the file.
            permission      : File permissions to be set. Example 0755.
            buffer_size     : Size of buffer used for data transfer.
            create_parent   : Creates parent folder before upload
                                    if set to True. Default is  False
            overwrite       : True will overwrite the containing file if exists. Default True.
            ignore_error    : If set to True will ignore failures
                                    and not raise exception.

        Returns:
            status of the transfer - True or False

		Example:
		from webhdfs import Webhdfs

        whdfs = Webhdfs(host=localhost, port=50070)
	    block_size = 64000000
	    buffer_size = 32000
	    srcdir = "/home/me"
	    tgtpath = "/user/me"
	    whdfs.upload_dir(srcdir, tgtpath, block_size=block_size,
                         buffer_size=buffer_size)

#### Append a file
    append_file(srcfile, tgtfile, buffer_size=None):
    
    Appends a local file to hdfs file
        Args:
            srcfile(str)       : Local source file to be uploaded.
            tgtfile(str)       : The tgtfile hdfs file to append.
            buffer_size(int)   : Size of buffer used for data transfer.
        Example:
			from webhdfs import Webhdfs
			
	        whdfs = Webhdfs(host=localhost, port=50070)
		    buffer_size = 32000
		    srcfile = "/home/me/x_1.txt"
		    tgtfile = "/user/me/x.txt"
		    whdfs.append_file(srcfile, tgtfile, 
							   buffer_size=buffer_size)

#### Append data iterator
     append_data_iter(data_iter, tgtfile, buffer_size=None, 
                 overwrite=True)
                    
	 Appends data produces by an iterator to hdfs directory
        Args:
            data_iter(iter)    : Iterator such as generator function.
            tgtfile(str)       : The target hdfs file.
            buffer_size(int)   : Size of target 
					             buffer used for data transfer.
	    Example:
			from webhdfs import Webhdfs
			
	        def gen_data(val=100000):
			    for i in range(1,val):
			        yield str(i) + "\n"
			    raise StopIteration()
			 
		    whdfs = Webhdfs(host=localhost, port=50070)
		    srcfile = "/home/me/x.txt"
		    tgtfile = "/user/me"
		    whdfs.append_data_ter(gen_data(), tgtfile, 
		                          buffer_size=buffer_size)

#### Concatenate Files
    concat_files(tgtfile, srcfilelist):
        Concatenates source files into target file.
        The source files will no longer exists are a successful concat
        Args:
            tgtfile(str)       : Target file
            srcfilelist(str)   : Source file/file list
        Example:
			from webhdfs import Webhdfs
			
	        whdfs = Webhdfs(host=localhost, port=50070)
		    srcfilelist = ["/user/me/x_1.txt", "/user/me/x_2.txt"]
		    tgtfile = "/user/me/x.txt"
		    whdfs.append_file(tgtfile, srcfilelist)


#### Get table list and metadata
	 Get All tables under database default as well as table metadata 
	 for "random_table" in database default.

     from webhcat import Webhcat 

     web_hcat_host = "webhcat.host.com" 
     web_hcat_port = 50111 
     database = "default" 
     table = "random_table" 
     whcat = Webhcat(host=web_hcat_host, port=web_hcat_port, 
                    default_database=database) 
     print whcat.get_tables(database) 
     print whcat.get_table_metadata(database, table) 

#### Scan a directory for large files 

    from webhdfs import Webhdfs
    web_hdfs_host = "namenode.host.com"
	size = 10000000000
    path = "/apps"
    whdfs = Webhdfs(host=web_hdfs_host, port=web_hdfs_port)
    for root, dirs, files in whdfs.scan_dir(path):
        for file in files:
	        if file['length'] > size:
	            print(root + "/" + file['pathSuffix'])

#### Print disk utilization for objects under a path

    from webhdfs import Webhdfs

    web_hdfs_host = "namenode.host.com"
    web_hdfs_port = 50070
    path = "/apps"
    whdfs = Webhdfs(host=web_hdfs_host, port=web_hdfs_port)
    for root, dirs, files in whdfs.scan_dir(path):
        print("{0} has size {1} and object count {2}"
               .format(root,
                       whdfs.get_content_summary(root)['length'],
                       len(dirs)+len(files)))

#### Initialize Hcat connection
    Webhcat(host=None, port=50111, protocol="http", user=None, 
                 password=None, url=None, default_database="default"):

    This initializes the webhcat connection string.   
	    Args:
	        protocol : The  protocol to be used to connect to source.
	                   The default is 'http'
	        host     : The host where webhcat service is running.
	                   This is a required argument.
	        user     : The user with which to connect to webhcat 
				       service.
	        password : The password with which to connect to webhcat 
				       service.
	        url    :   url which is a combination of protocol, host and 
				       port.
          
      Example:
	    from webhcat import Webhcat
		whcat = Webhcat(host=webhcat.host.com, port=50111)

#### Get databases
     get_databases(pattern=None)
     
     Returns the list of database names in the hive metastore.
     Args:
         pattern : The regular expression search pattern for database 
			         names. This parameter is optional.
	Example:
		from webhcat import Webhcat
		whcat = Webhcat(host=webhcat.host.com, port=50111)
	    print(whcat.get_databases,"prod*")
	
#### Get database metadata
     get_database_metadata(database=None)
     
     Returns the metadata associated to the provided hive database.
     Args:
         database : This parameter is required and represents the 
			        database whose metadata information needs to be 
			        obtained. Default is what is specified in the 
			        connection string or "default"
			        
     Returns:
         {
          "location": <location on hdfs>,
          "params": "{key=val}",
          "comment": <comment>,
          "database": <database name>
         }   
         
	 Example:
		from webhcat import Webhcat
		whcat = Webhcat(host=webhcat.host.com, port=50111, 
						database="prod")
	    print(whcat.get_database_metadata())

#### Get tables
     get_tables(database=None,pattern=None)
     
	 Returns  the list of tables in a database.   
     Args:
         database : The name of the database. The default database.
         pattern  : The glob expression search pattern for table names.
                    This parameter is optional.
     Returns:
         list of table names under the database. 
         
	 Example:
		from webhcat import Webhcat
		whcat = Webhcat(host=webhcat.host.com, port=50111)
	    for table in get_tables(database="prod"):
		    print(table)

### Create table
	create_table(table, columns, location=None, database=None,
                 group=None, permission=None, is_external=False,
                 comment=None, partition_by=None, cluster_by=None,
                 format=None, table_properties=None, 
                 ignore_error=False)
                 
	Creates a partition for a existing table.
        Args:
	      table(str)  : The table name who's metadata is
             needed.
          columns(list[dict]) : The list of column names and 
	               types. Example shown below.
                   [
                       { "name": <cname>, "type": <ctype> },
                       { "name": <cname>, "type": <ctype>,
	                         "comment": <comment> }
                   ]
		  database(str)       : The name of the database.
		  group(str)          : user group for partition creation.
		  permission(str/int) : The permissions string to use.
		                        The format is "rwxrw-r-x"
		  is_external(Bool)   : Bool
		  comment(str)        : Comment for the table
		  location(str)       : Physical location of the partition
		  partition_by(list[dict]) : Partition column details and types 
						  as a list of dictionaries
							 [{ "name": "country", "type": "string" }]
		  cluster_by(dict)    : Column details for clustering. 
							  Example shown below.
	                          {
	                            "columnNames": [<list of columns>],
	                            "sortedBy": [{ "columnName": 
							                            <cname>,
					                             "order": <ASC/DESC>}],
	                            "numberOfBuckets": <K>
	                          }
		  format(dict)   : Table format details as a dictionary
                         {
                            "storedAs": <fileformat example rcfile>,
                            "rowFormat":
                             {
                               "fieldsTerminatedBy": <field delimiter>,
                               "serde": {
                                        "name": <serde>,
                                        "properties": {"key": "value" }
                                        }
                             }
                           }
		  table_properties(dict)     : dictionary of key:value table 
								          properties.
		  ignore_error               : If true, no error if the table 
								         already exists.

	 Example:
		from webhcat import Webhcat
		whcat = Webhcat(host=webhcat.host.com, port=50111)
        comment = "test table"
        columns = [{"name": "id", "type": "bigint"},
                   {"name": "price", "type": "float", 
                   "comment": "The unit price"}]
        database = "default"
        partition_by = [{"name": "country", "type": "string"}]
        format = {"storedAs": "rcfile"}
        whcat.create_table(table="test2", columns=columns, 
						   database=self.database,comment=comment,
						   partition_by=partition_by,
						   cluster_by=None,format=format))
### Drop table
    drop_table(database=None, table=None)
    
    Drops hive table.
       Args:
          database(str) : The name of the database. Optional.
          table  : The table that needs to be dropped.
          
       Examples:
       	  from webhcat import Webhcat
		  whcat = Webhcat(host=webhcat.host.com, port=50111)
		  whcat.drop_table(database="default", table="test1")

### Copy table
    copy_table(database=None, table=None, new_table=None,
			   group=None, permission=None, location=None, 
			   is_external=False, ignore_error=False):
    
    Creates a new hive table like an existing hive table.
    Only metadata is copied.
       Args:
           database(str) : The name of the database. Optional.
           table         : The table that needs to be renamed.
           new_table     : New table name.
           group         : user group.
           permission    : The permissions string to use. 
					       The format is "rwxrw-r-x"
           location(str) : Physical location of the partition
           is_external(Bool)   : Bool
           ignore_error   : If true, no error if the table already 
				           exists.
          
       Examples:
       	  from webhcat import Webhcat
		  whcat = Webhcat(host=webhcat.host.com, port=50111)
		  whcat.copy_table(database="default", table="test1", 
						  new_table="test2")

### Rename table
    rename_table(database=None, table=None, rename=None
                 group=None, permission=None)

    Renames hive table.
       Args:
          database(str) : The name of the database. Optional.
          table  : The table that needs to be renamed.
          rename  : New name for the table.
          group  : user group.
          permission    : The permissions string to use. The format is "rwxrw-r-x"
       Examples:
       	  from webhcat import Webhcat
		  whcat = Webhcat(host=webhcat.host.com, port=50111)
		  whcat.rename_table(database="default", table="test1", rename="test2")

### Add partition
    add_partition(table, partition,
                  location, database=None,
                  group=None, permission=None,
                  ignore_error=False)
    
    Creates a partition for an existing table.
	    Args:
          table(str)    : The table name who's metadata is needed.
          database(str) : The name of the database.
          location(str) : Physical location of the partition
          partition(str): Name of the partition as key=value
                          value is quoted if string.
                          Example: "country='algeria'"
          permission(str): The permissions string to use.
                           The format is "rwxrw-r-x"
          group(str)    : user group for partition creation.
    
	    Example:
	      from webhcat import Webhcat
		  whcat = Webhcat(host=webhcat.host.com, port=50111)
	      location = "/tmp/country/algeria"
		  partition = "country='algeria'"
		  whcat.add_partition(table, partition,
		                      location, database=database)

### Get partition
    get_partition(table, partition, database=None)
    
        Get information about a single partition for a existing table.
           Args:
               table(str)    : The table name who's metadata is needed.
               partition(str): Name of the partition as key=value
                               value is quoted if string.
                               Example: "country='algeria'"
               database(str) : The name of the database. Optional    
	    Example:
	      from webhcat import Webhcat
		  whcat = Webhcat(host=webhcat.host.com, port=50111)
	      location = "/tmp/country/algeria"
		  partition = "country='algeria'"
		  print(whcat.get_partition(table, partition, 
									database=database))
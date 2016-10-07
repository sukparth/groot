# import requests
# from requests.exceptions import HTTPError
import requests
from request import Request
from request import url_join
from errors import HTTPError, RequestError, MissingArgumentError, IllegalArgumentError
import os
import time
import getpass
import operator
import json
from util import regex_search, glob_search, merge_dict
from search import SearchKeys, SearchOperators, SearchLogicalOperators
from search import SearchExpression, SearchExpressionList


class WhdfsSearchKeys(SearchKeys):
    """
    List of search key constants and their mappings in the webhdfs filestatus
    """
    PATH_KEY = "name"
    MTIME_KEY = "mtime"
    OWNER_KEY = "owner"
    REPL_KEY = "replication"
    TYPE_KEY = "type"
    SIZE_KEY = "length"
    DIR_COUNT_KEY = "num_dir"
    FILE_COUNT_KEY = "num_file"
    COUNT_QUOTA_KEY = "count_quota"
    RAW_SIZE_KEY = "raw_length"
    SPACE_QUOTA_KEY = "space_quota"
    SEARCH_KEY_DICT = {DIR_COUNT_KEY: "directoryCount",
                       FILE_COUNT_KEY: "fileCount",
                       COUNT_QUOTA_KEY: "quota",
                       SPACE_QUOTA_KEY: "spaceQuota",
                       RAW_SIZE_KEY: "spaceConsumed",
                       PATH_KEY: "pathSuffix",
                       MTIME_KEY: "modificationTime",
                       OWNER_KEY: "owner",
                       REPL_KEY: "replication",
                       TYPE_KEY: "type",
                       SIZE_KEY: "length"}


class WhdfsSearchOperators(SearchOperators):
    """
    List of operations and their mappings to functions
    """
    OPER_DICT = {'>': operator.gt,
                 '<': operator.lt,
                 '>=': operator.ge,
                 '<=': operator.le,
                 '=': operator.eq,
                 'regex': regex_search,
                 'glob': glob_search}


class WhdfsSearchLogicalOperators(SearchLogicalOperators):
    pass


class WhdfsSearchExpression(SearchExpression):
    """ Contains a list of tokens in postfix order."""

    def __init__(self, key, oper, val):
        super(WhdfsSearchExpression, self).__init__(key, oper, val, WhdfsSearchKeys, WhdfsSearchOperators)


class WhdfsSearchExpressionList(SearchExpressionList):
    """ Contains a list of tokens in postfix order."""

    def __init__(self, search_expression=None):
        super(WhdfsSearchExpressionList, self).__init__(WhdfsSearchLogicalOperators, search_expression)

    def add(self, token):
        """ Adds the token to the list if valid """
        super(WhdfsSearchExpressionList, self).add(token)

    def add_expression(self, expression):
        """ Parser the expression and add it.
        Arguments:
            expression(list) : An expression list in infix format
                               The list can contain
                               1) Curved braces
                               2) Search Expression of type SearchExpression or its derivative
                               3) Logical Operator with string value "and" or "or"
        """
        super(WhdfsSearchExpressionList, self).add_expression(expression)
        pass


class Webhdfs(object):
    """ Represent class for webhdfs connectivity
    """

    def __init__(self, host=None, port=50070, url=None, protocol="http",
                 url_ext="webhdfs/v1", user=None):
        """ Initialization for class object.
            Args:
                protocol(str) : The  protocol to be used to connect to webhdfs.
                                The default is 'http'
                host(str)     : The host where webhdfs service is running.
                  or list)      This is a required argument. This can be a single host,
                                a string containing hosts in a HA separated by comma(,) or
                                a list of hosts in a HA setup.
                                The connection attempt will be made in order from left to right.
                port          : The port on which webhdfs is listening.
                url           : Url which is a combination of protocol, host and port
        """
        if (not host or not port) and not url:
            raise MissingArgumentError("Either url or a combination of host and port " +
                                       "need to be provided")

        if not protocol:
            raise MissingArgumentError("protocol argument should have a value")

        if not url_ext:
            raise MissingArgumentError("database_ext argument needs to have a value")

        self.url_ext = url_ext
        self.user = user or getpass.getuser()
        self.response_timeout = 20

        hosts = host
        if url:
            self.base_url = url
            try:
                self.list_dir(path="/")
            except:
                raise RequestError("Cannot connect to webhdfs service at {0}"
                                   .format(self._get_path_url(path="/")))
        elif isinstance(hosts, str) or (isinstance(host, list) or isinstance(host, tuple) or isinstance(host, set)):
            if isinstance(hosts, str):
                hosts = [x.strip() for x in hosts.split(",")]
            nnstatus = False
            for host in hosts:
                self.base_url = protocol + "://" + host + ":" + str(port)
                try:
                    self.list_dir(path="/")
                    nnstatus = True
                    break
                except:
                    pass

            if nnstatus is False:
                raise RequestError("Cannot connect to webhdfs service at {0}"
                                   .format(self._get_path_url(path="/")))

    def _get_path_url(self, path):
        """
        Helper method
        Returns the path access url
        Example:
        Input:
        -------------
        x/y
        Returns:
        ---------
        http://webhdfs.host.com:50070/webhdfs/v1/X/y
        """
        if not path.startswith("/"):
            path = "/" + path
        return url_join(self.base_url, self.url_ext) + path

    def _get_op_url(self, url, operation):
        """
        Helper method
        Returns the operation restapi call string
        Example:
        Input:
        -------------
        url = http://webhdfs.host.com:50070/webhdfs/v1/x/y
        op  = LISTSTATUS
        Returns:
        ---------
        http://webhdfs.host.com:50070/webhdfs/v1/x/y?user.name=guest&op=LISTSTATUS
        """
        return url + "?user.name=" + self.user + "&op=" + operation

    def _list_attribute(self, path_status, key):
        """
        Helper method
        Returns path_status attribute if key is provided or path_status
        itself is key is None or has the value "all"
        """
        return path_status if (key is None or key.lower() == "all") else \
            path_status[WhdfsSearchKeys.get_value(key)]

    def url_json_request(self, url, method=None, data=None,
                         ignore_error=False):
        """
        Submit a url request to webhdfs which returns a  json response.
        Parameters
        ---------------
        url    : Entire url
        method : None, PUT, POST, DELETE or GET
        data : Any payload that needs to be passed
        ignore_error  : Ignores webhdfs remote exceptions and return empty json

        Returns
        ---------
        Response in json format
        """

        response = requests.Response()
        if not url:
            raise MissingArgumentError("URL not provided")

        err_msg = ""
        try:
            response = Request.url_request(url, method=method, data=data, timeout=self.response_timeout)
            json_response = response.json()
            if "RemoteException" in json_response:
                err_msg = json_response['RemoteException']['message']
                if 'Invalid value for webhdfs parameter \"op\": No enum constant' in err_msg:
                    err_msg = "Operation not supported by webhdfs service. You may trying to" + \
                              " access an earlier version of hadoop."
                raise HTTPError(err_msg)
            # print json_response
            return json_response
        except HTTPError as e:
            if not ignore_error:
                raise IOError("Error Reported in url request call \n{0}\n"
                              .format(e.message))
            else:
                return {}
        except ValueError:
            if not ignore_error:
                raise IOError("Error Reported in url request call \n{0}\n"
                              .format(response.text))
            else:
                return {}

    def is_exists(self, path):
        """
        Checks if hdfs path exists.
        Parameters
        ---------------
        path    : The path to check

        Return
        ---------
        True or False
        """

        if not path:
            raise MissingArgumentError("Path not provided")

        try:
            path_key = WhdfsSearchKeys.get_value(WhdfsSearchKeys.PATH_KEY)
            path_info = self.get_path_status(path)
            # print(path_info)
            if path_key in path_info:
                return True
            else:
                return False
        except IOError:
            return False

    def is_file(self, path):
        """
        Checks if hdfs path is a file.
        Parameters
        ---------------
        path    : The path to check

        Returns
        ---------
        True or False
        """

        if not path:
            raise MissingArgumentError("Path not provided")

        try:
            type_key = WhdfsSearchKeys.get_value(WhdfsSearchKeys.TYPE_KEY)
            path_info = self.get_path_status(path)
            if type_key \
                    in path_info:
                if path_info[type_key] == "FILE":
                    return True
                else:
                    return False
            else:
                return False
        except IOError:
            return False

    def is_dir(self, path):
        """
        Checks if hdfs path is a directory.
        Parameters
        ---------------
        path    : The path to check
        Returns
        ---------
        True or False
        """

        if not path:
            raise MissingArgumentError("Path not provided")

        try:
            type_key = WhdfsSearchKeys.get_value(WhdfsSearchKeys.TYPE_KEY)
            path_info = self.get_path_status(path)
            if type_key in path_info:
                if path_info[type_key] == "DIRECTORY":
                    return True
                else:
                    return False
            else:
                return False
        except IOError:
            return False

    def is_symlink(self, path):
        """
        Checks if hdfs path is a symlink.
        Args:
            path    : The path to check
        Returns:
            True or False
        """

        if not path:
            raise MissingArgumentError("Path not provided")

        try:
            type_key = WhdfsSearchKeys.get_value(WhdfsSearchKeys.TYPE_KEY)
            path_info = self.get_path_status(path)
            if type_key in path_info:
                if path_info[type_key] == "SYMLINK":
                    return True
                else:
                    return False
            else:
                return False
        except IOError:
            return False

    def get_path_status(self, path, ignore_error=False):
        """
        Get path information
        Args:
            path                 : Path
            ignore_error(bool)   : ignore error
        Returns:
            Long listing of file system objects of the below dictionary format
            {
                  u'group': u'XXX', u'permission': u'NNN',
                  u'blockSize': N, u'accessTime': N, u'pathSuffix': u'XYZ',
                  u'modificationTime': NNNNNNNL, u'replication': N,
                  u'length': N, u'childrenNum': N, u'owner': u'user',
                  u'storagePolicy': N, u'type': u<'DIRECTORY', 'FILE' or 'SYMLINK'>,
                  u'fileId': NNNNNNN
             }
        """

        if not path:
            raise MissingArgumentError("Path not provided")

        list_dir_op = "GETFILESTATUS"
        url = self._get_op_url(self._get_path_url(path), list_dir_op)
        json_fileinfo = self.url_json_request(url, ignore_error=ignore_error)
        if "FileStatus" in json_fileinfo:
            json_fileinfo = json_fileinfo["FileStatus"]
        return json_fileinfo

    def get_content_summary(self, path, ignore_error=False):
        """
        Get path information
        Arguments:
            path       : Path
            ignore_error(bool)   : ignore error if set to True. Default False.

        Returns:
            Content Summary of the path as a dictionary
            {
                "directoryCount":n, "fileCount":n,
                "length":n,"quota":n,
                "spaceConsumed":n,"spaceQuota":n
            }
        """
        if not path:
            raise MissingArgumentError("Path not provided")

        cs_op = "GETCONTENTSUMMARY"
        url = self._get_op_url(self._get_path_url(path), cs_op)
        json_fileinfo = self.url_json_request(url, ignore_error=ignore_error)
        if "ContentSummary" in json_fileinfo:
            return json_fileinfo["ContentSummary"]
        else:
            return {}

    def make_dirs(self, path, permission=None):
        """ Create a directory including parents if it does not exist.

        Args:
            path(str)         : Directory path to create
            permission(int)   : Octal permissions example 1777
        """

        if not path:
            raise MissingArgumentError("Path not provided")

        list_dir_op = "MKDIRS"
        if permission:
            list_dir_op += "&permission=" + str(permission).strip()
        url = self._get_op_url(self._get_path_url(path), list_dir_op)

        self.url_json_request(url, method="put")

    def make_dir(self, path, permission=None):
        """
        Create a directory in safe mode.
        If the directory exists or the parent directories do not exist
        it will throw an exception.

        Args:
            path(str)         : Directory path to create
            permission(int)   : Octal permissions example 1777
        """

        if not path:
            raise MissingArgumentError("Path not provided")

        make_dir_op = "MKDIRS"
        if permission:
            make_dir_op += "&permission=" + str(permission)
        if self.is_exists(path):
            raise IOError("Path {0} already exists".format(path))

        parent_path = os.path.dirname(path)
        parent_path = parent_path.replace("//", "/")

        if not self.is_exists(parent_path):
            raise IOError("Parent Path {0} does not exists"
                          .format(parent_path))

        url = self._get_op_url(self._get_path_url(path), make_dir_op)
        self.url_json_request(url, method="PUT")

    def create_symlink(self, path, destination, create_parent=False):
        """
        Change owner of path.
        Args:
            path(str)                : Symlink path
            destination(str)         : target path
            create_parent(str)       : Creates parent directory for symlink.
                                       Default value is False.
        """

        if not path:
            raise MissingArgumentError("Path not provided")
        if not destination:
            raise MissingArgumentError("Target not provided")

        create_sym_op = "CREATESYMLINK&destination=" + destination
        if create_parent:
            create_sym_op += "&createParent=true"
        url = self._get_op_url(self._get_path_url(path), create_sym_op)
        Request.url_request(url, method="PUT")

    def delete(self, path, recursive=False):
        """
        Delete a path.
        Args:
            path(str)         : Path on HDFS to rename
            recursive(Bool)   : If True will recursively delete. Default False

        """
        if not path:
            raise MissingArgumentError("Path not provided")
        delete_op = "DELETE&recursive=" + str(recursive).lower()

        url = self._get_op_url(self._get_path_url(path), delete_op)
        self.url_json_request(url, method="DELETE")

    def rename(self, path, newpath):
        """
        Move or rename a file orr directory
        Args:
            path(str)         : Path on HDFS to rename
            newpath(str)      : Rename Path
        """

        if not path:
            raise MissingArgumentError("Path not provided")
        if not newpath:
            raise MissingArgumentError("Rename Path not provided")

        rename_op = "RENAME&destination=" + newpath
        url = self._get_op_url(self._get_path_url(path), rename_op)
        self.url_json_request(url, method="PUT")

    def change_owner(self, path, owner=None, group=None):
        """
        Change owner of path.
        Args:
            path(str)         : Path on HDFS to rename
            owner(str)        : owner
            group(str)        : group
        """
        if not path:
            raise MissingArgumentError("Path not provided")
        change_owner_op = "SETOWNER"
        if owner:
            change_owner_op += "&owner=" + owner
        if group:
            change_owner_op += "&group=" + group
        url = self._get_op_url(self._get_path_url(path), change_owner_op)
        Request.url_request(url, method="PUT")

    def change_perm(self, path, permission):
        """
        Change owner of path.
        Args:
            path(str)             : Path on HDFS to rename
            permission(int/str)   : Permission in octal
        """
        if not path:
            raise MissingArgumentError("Path")
        if not permission:
            raise MissingArgumentError("Permission")
        change_perm_op = "SETPERMISSION&permission=" + str(permission)
        url = self._get_op_url(self._get_path_url(path), change_perm_op)
        Request.url_request(url, method="PUT")

    def _build_extended_info(self, dir_count=0, file_count=0, count_quota=-1,
                             space_quota=-1, raw_size=0):
        return \
            {WhdfsSearchKeys.get_value(WhdfsSearchKeys.DIR_COUNT_KEY): dir_count,
             WhdfsSearchKeys.get_value(WhdfsSearchKeys.FILE_COUNT_KEY): file_count,
             WhdfsSearchKeys.get_value(WhdfsSearchKeys.COUNT_QUOTA_KEY): count_quota,
             WhdfsSearchKeys.get_value(WhdfsSearchKeys.SPACE_QUOTA_KEY): space_quota,
             WhdfsSearchKeys.get_value(WhdfsSearchKeys.RAW_SIZE_KEY): raw_size}

    def _list_dir_info(self, path=None, otype="all", key=None,
                       ignore_error=False, search_exp_list=None,
                       ext_status=False):

        """ Helper function to list directory information
        type == "all" will list all contents
        type = "file" will list only files and symlinks
        type = "dir" will list only dirs
        key represents an information field to be searched
        out : dictionary with list of file information
        ignore_error : Ignores url errors which includes errors reported
                    from wbhdfs
        search_exp_list(WhdfsSearchExpressionList) : Search expression list.
        """
        list_dir_op = "LISTSTATUS"
        url = self._get_op_url(self._get_path_url(path), list_dir_op)
        tmap = {"file": "FILE", "dir": "DIRECTORY", "symlink": "SYMLINK"}

        try:
            json_dirlist = \
                self.url_json_request(url, ignore_error=ignore_error)

            if not json_dirlist or "RemoteException" in json_dirlist:
                if not ignore_error:
                    raise IOError("{0} does not exist or ".format(path) +
                                  "or {0} does not have ".format(self.user) +
                                  "permissions to access it.")
            elif otype == "all":
                if ext_status:
                    outlist = []
                    for x in json_dirlist['FileStatuses']['FileStatus']:
                        if not search_exp_list or search_exp_list.match(x):
                            type_key = WhdfsSearchKeys.get_value(WhdfsSearchKeys.TYPE_KEY)
                            path_key = WhdfsSearchKeys.get_value(WhdfsSearchKeys.PATH_KEY)
                            if path == "/":
                                fullpath = "" + x[path_key]
                            else:
                                fullpath = path + "/" + x[path_key]
                            # print "fullpath is " + fullpath + " " + x[type_key] + " " + tmap['dir']
                            if x[type_key] == tmap['dir']:
                                ext_x = merge_dict(x,
                                                   self.get_content_summary(fullpath,
                                                                            ignore_error=ignore_error) or
                                                   self._build_extended_info())
                            elif x[type_key] == tmap['file'] or x[type_key] == tmap['symlink']:
                                replication_key = WhdfsSearchKeys.get_value(WhdfsSearchKeys.REPL_KEY)
                                size_key = WhdfsSearchKeys.get_value(WhdfsSearchKeys.SIZE_KEY)
                                size = x[size_key]
                                replication = x[replication_key]
                                ext_x = merge_dict(x, self._build_extended_info(0, 1, raw_size=size * replication))
                            else:
                                ext_x = merge_dict(x, self._build_extended_info())
                            outlist.append(self._list_attribute(ext_x, key))
                    return outlist
                else:
                    return [self._list_attribute(x, key) for x in
                            json_dirlist['FileStatuses']['FileStatus']
                            if not search_exp_list or search_exp_list.match(x)]
            else:
                type_key = WhdfsSearchKeys.get_value(WhdfsSearchKeys.TYPE_KEY)
                path_key = WhdfsSearchKeys.get_value(WhdfsSearchKeys.PATH_KEY)
                if ext_status:
                    outlist = []
                    for x in json_dirlist['FileStatuses']['FileStatus']:
                        if x[type_key] == tmap[otype] and \
                                (not search_exp_list or search_exp_list.match(x)):
                            if path == "/":
                                fullpath = "" + x[path_key]
                            else:
                                fullpath = path + "/" + x[path_key]
                            if otype == "dir":
                                ext_x = merge_dict(x,
                                                   self.get_content_summary(fullpath, ignore_error=ignore_error))
                            elif otype == "file":
                                replication_key = WhdfsSearchKeys.get_value(WhdfsSearchKeys.REPL_KEY)
                                size_key = WhdfsSearchKeys.get_value(WhdfsSearchKeys.SIZE_KEY)
                                size = x[size_key]
                                replication = x[replication_key]
                                ext_x = merge_dict(x, self._build_extended_info(0, 1, raw_size=size * replication))
                            else:
                                ext_x = merge_dict(x, self._build_extended_info())
                            outlist.append(self._list_attribute(ext_x, key))
                    return outlist
                else:
                    return [self._list_attribute(x, key) for x in
                            json_dirlist['FileStatuses']['FileStatus']
                            if (x[type_key] == tmap[otype] and
                                (not search_exp_list or
                                 search_exp_list.match(x)))]
        except IOError:
            if not ignore_error:
                raise

    def list_dir(self, path=None, otype="all", pattern=None,
                 pattern_type="glob", ignore_error=False):
        """ List contents of directory
            Args:
            path(str)         : Full path to start the recursive listing
            otype(str)        : Object type
                                otype = "all" will list all contents
                                otype = "file" will list only files
                                otype = "dir" will list only dirs
            pattern(str)      : Search for files/dirs of specific pattern
                                based on pattern_type .
            pattern_type(str) : "glob" or "regex". Default glob.
            ext_status(bool)  : True means get extended statistics. Default False

            Returns:
                List of files/directories names under path.
        """
        if not path:
            raise MissingArgumentError("Path not provided")
        search_exp_list = None
        if pattern:
            search_exp_list = \
                WhdfsSearchExpressionList(WhdfsSearchExpression
                                          (WhdfsSearchKeys.PATH_KEY,
                                           pattern_type,
                                           pattern))
        return self._list_dir_info(path=path, otype=otype,
                                   key=WhdfsSearchKeys.PATH_KEY,
                                   ignore_error=ignore_error,
                                   search_exp_list=search_exp_list)

    def long_list_dir(self, path=None, otype="all", pattern=None,
                      pattern_type="glob", ignore_error=False,
                      ext_status=False):
        """ List contents of directory

        Args:
            path(str)          : Full path to start the recursive listing
            otype(str)         : Object type
                                 otype = "all" will list all contents
                                 otype = "file" will list only files
                                 otype = "dir" will list only dirs
            pattern(str)       :  Search for files of specific pattern
                                 based on pattern_type .
            pattern_type(str)  :  "glob" or "regex". Default glob.
            ignore_error(bool) : Ignore any errors during search such as access issues.
            ext_status(bool)   : Produces extra status information namely content
                                 summary for directories.

        Returns:
        List of dictionaries containing file/directory status information as
        {
              u'group': u'XXX', u'permission': u'777',
              u'blockSize': N, u'accessTime': N, u'pathSuffix': u'XYZ',
              u'modificationTime': NNNNNNNL, u'replication': N,
              u'length': N, u'childrenNum': N, u'owner': u'user',
              u'storagePolicy': N, u'type': u<'DIRECTORY' or 'FILE'>,
              u'fileId': NNNNNNN
         }
        """
        search_exp_list = None
        if pattern:
            search_exp_list = \
                WhdfsSearchExpressionList(WhdfsSearchExpression
                                          (WhdfsSearchKeys.PATH_KEY,
                                           pattern_type,
                                           pattern))

        return self._list_dir_info(path=path, otype=otype, key="all",
                                   ignore_error=ignore_error,
                                   search_exp_list=search_exp_list,
                                   ext_status=ext_status)

    def move(self, srcpath, tgtpath, pattern, pattern_type="glob"):
        """
        Move or rename a file ir directory
        Parameters
        ----------------
        srcpath(str)         : Source Path on HDFS to rename
        tgtpath(str)         : Renamed  Target Path
        """
        if not tgtpath:
            raise MissingArgumentError("Target Path")
        if not srcpath:
            raise MissingArgumentError("Source Path")
        src_is_dir = self.is_dir(srcpath)  # Source is a directory
        if src_is_dir and not self.is_file(tgtpath):
            raise IOError("Source path {0} is a directory and ".format(srcpath) +
                          "target path {0} is a file".format(tgtpath))
        if src_is_dir:
            if pattern:
                for f in self.list_dir(srcpath, pattern, pattern_type):
                    target = tgtpath + "/" + f
                    self.rename(srcpath, target)
            else:
                self.rename(srcpath, tgtpath)
        else:  # Source is a file
            self.rename(srcpath, tgtpath)

    def concat_files(self, tgtfile, srcfilelist):
        """
        Concatenates source files into target file.
        The source files will no longer exists are a successful merge
        Args:
            tgtfile(str)       : Target file
            srcfilelist(str)   : Source file/file list
        """
        if not tgtfile:
            raise MissingArgumentError("Target File")
        if not srcfilelist:
            raise MissingArgumentError("Source File List")
        if isinstance(srcfilelist, list):
            concat_op = "CONCAT&sources=" + ",".join(srcfilelist)
        else:
            concat_op = "CONCAT&sources=" + srcfilelist
        url = self._get_op_url(self._get_path_url(tgtfile), concat_op)
        Request.url_request(url, method="POST")

    def _scan_dir(self, fullpath, pathinfo, otype, currlev,
                  tmap, level, pattern, pattern_type="glob",
                  ignore_error=True, search_exp_list=None,
                  skip_dir_list=set([]), search_scan_flag=False):
        """
        Helper method that generates values for the scan_dir method.
        This method is a generator and yields returns of format
        root, dirlist, filelist
        """
        if fullpath not in skip_dir_list:
            type_key = WhdfsSearchKeys.get_value(WhdfsSearchKeys.TYPE_KEY)
            path_key = WhdfsSearchKeys.get_value(WhdfsSearchKeys.PATH_KEY)
            if currlev <= level and \
                            pathinfo[type_key] == tmap["dir"]:
                try:
                    olist = self.long_list_dir(path=fullpath, otype="all",
                                               ignore_error=ignore_error,
                                               ext_status=True)
                    if not olist:
                        yield fullpath, [], []
                    else:
                        dlist = filter(lambda x:
                                       x[type_key] ==
                                       tmap["dir"], olist)
                        if otype == "all" or otype == "file":
                            outflist = \
                                filter(lambda x:
                                       x[type_key] ==
                                       tmap["file"] and
                                       (not search_exp_list or
                                        search_exp_list.match(x)),
                                       olist)
                        else:
                            outflist = []

                        if otype == "all" or otype == "dir":
                            outdlist = \
                                filter(lambda x: (not search_exp_list or
                                                  search_exp_list.match(x)),
                                       dlist)
                        else:
                            outdlist = []

                        yield fullpath, outdlist, outflist
                        if fullpath == "/":
                            fullpath = ""
                        for npathinfo in dlist:
                            for root, dirs, files in \
                                    self._scan_dir(fullpath + "/" +
                                                           npathinfo[path_key],
                                                   npathinfo, otype, currlev + 1,
                                                   tmap, level, pattern,
                                                   pattern_type, ignore_error,
                                                   search_exp_list, skip_dir_list,
                                                   search_scan_flag):
                                yield root, dirs, files
                except:
                    raise

    def scan_dir(self, path, level=None, pattern=None, pattern_type="glob",
                 otype="all", ignore_error=True, skip_dirs=[],
                 search_exp_list=None):
        """
        Walk through the filesystem starting from path with long listing for each
        filesystem object.

        Args:
            path(str)           : Full path to start the recursive listing
            level(int)          : Maximum depth of directories from the path to list
            pattern(str)        : Search for files of specific pattern
                                    based on pattern_type .
            pattern_type(str)   : "glob" or "regex". Default glob.
            otype(str)          : File, dir or all. Default all
            ignore_error(bool)  : Default set to True. If set to false when it encounters
                                    a object or directory it cannot accept it will fail.
            skip_dirs           : List of directory to skip recursing into.
            search_exp_list(WhdfsSearchExpressionList) : Search expression list.

        Returns:
            Generator of type root, dirlist, filelist where dirlist and filelist
            contains status of directories and files under root respectively.

        Example:
        #Scan directories printing out parent, child directories and files
            from dpwebhdfs import Webhdfs

            whdfs = Webhdfs(host=localhost, port=50070)
            for root,dirs,files in whdfs.scan_dir(path,otype = "all",level = 0):
                print root
                for dir in dirs['pathSuffix']:
                    print dir
                for file in files['pathSuffix']:
                    print file
        """

        tmap = {"file": "FILE", "dir": "DIRECTORY"}
        currlev = 1
        if level is None or level < 1:
            level = 500
        if not path:
            raise MissingArgumentError("Path not provided")
        try:
            pathinfo = self.get_path_status(path)
        except:
            raise
        if pattern or search_exp_list:
            search_scan_flag = True
        else:
            search_scan_flag = False

        if skip_dirs:
            if isinstance(skip_dirs, str):
                skip_dirs = set([skip_dirs.rstrip("/")])
            elif isinstance(skip_dirs, list):
                skip_dirs = set([x.rstrip("/") for x in skip_dirs])
            else:
                raise IllegalArgumentError("Skip dir list is not valid")

        if pattern:
            if not search_exp_list:
                path_search_exp = \
                    WhdfsSearchExpression(WhdfsSearchKeys.PATH_KEY,
                                          pattern_type,
                                          pattern)
                search_exp_list = WhdfsSearchExpressionList(path_search_exp)

        return self._scan_dir(path, pathinfo, otype, currlev,
                              tmap, level, pattern,
                              pattern_type, ignore_error=ignore_error,
                              search_exp_list=search_exp_list,
                              skip_dir_list=skip_dirs,
                              search_scan_flag=search_scan_flag)

    def iter_file(self, srcfile, length=None, buffer_size=None):
        """ Returns an read iterator over hdfs file
        Args:
            srcfile(str)       : hdfs file to be print
            length(int)        : length to read
            buffer_size         : buffer size for each read
        """
        if not srcfile:
            raise MissingArgumentError("Source file not provided")
        open_op = "OPEN"
        if length:
            open_op += "&length=" + str(length)
        if buffer_size:
            open_op += "&buffer_size=" + str(buffer_size)
        url = self._get_op_url(self._get_path_url(srcfile), open_op)
        r = Request.url_request(url, method="get", stream=True,
                                allow_redirects=True)
        buffer_size = buffer_size or 4092
        return r.iter_content(chunk_size=buffer_size)

    def print_file(self, srcfile, length=None, buffer_size=None):
        """ Print contents of hdfs file
        Args:
            srcfile(str)       : hdfs file to be print
            length(int)        : length to read
            buffer_size         : buffer size for read
        """
        import sys
        for block in self.iter_file(srcfile=srcfile,
                                    length=length,
                                    buffer_size=buffer_size):
            if block:  # filter out keep-alive new chunks
                sys.stdout.write(block)

    def download_file(self, srcfile, tgtpath, overwrite=True,
                      ignore_error=False):
        """ Downloads hdfs file to local directory

        Args:
            srcfile(str)       : hdfs file to be downloaded
            tgtpath(str)       : The target directory
            overwrite(bool)    : When overwrite is set to True it will replace
                                    target file if it exists.
            ignore_error(bool  : If set to True will ignore download and other checks.
        """

        if not srcfile:
            raise MissingArgumentError("Source file not provided")
        if not tgtpath:
            raise MissingArgumentError("Target Path not provided")

        skip = False
        open_op = "OPEN"
        filename = os.path.basename(srcfile)

        if os.path.exists(tgtpath):
            target = tgtpath
            if os.path.isdir(target):
                target = os.path.join(target, filename)
            if os.path.isfile(target):
                if not overwrite:
                    skip = True
        else:
            tgtdir = os.path.dirname(tgtpath)
            if not os.path.isdir(tgtdir):
                if not ignore_error:
                    raise IOError("Directory {0} for not exist".format(tgtpath))
                else:
                    return False
            target = tgtpath

        url = self._get_op_url(self._get_path_url(srcfile), open_op)
        fileinfolist = self.long_list_dir(srcfile)
        if not fileinfolist:
            if not ignore_error:
                raise IOError("srcfile {0} does not exist"
                              .format(srcfile))
            else:
                return False
        srcsize = int(fileinfolist[0]['length'])
        # print url,target
        if skip is False:
            try:
                start = time.time()
                print("Downloading {0} to {1}".format(srcfile, tgtpath))
                Request.url_file_download(url, target)
                if not os.path.exists(target):
                    if not ignore_error:
                        raise IOError("Download failed")
                    else:
                        return False
                end = time.time()
                targetsize = os.path.getsize(target)
                if targetsize != srcsize:
                    if not ignore_error:
                        raise IOError("source size for file {0} is {1} "
                                      .format(srcfile, srcsize) +
                                      "target size for file {0} is {1}\n"
                                      .format(target, targetsize) +
                                      "File sizes differ. Download failed.")
                    else:
                        return False
                else:
                    print("Total time taken to download {0} bytes ".format(srcsize) +
                          "is {0} seconds ".format(str(end - start).strip()))
            except:
                if not ignore_error:
                    raise
                else:
                    print("Download failed".format(srcfile, tgtpath))
                    return False
        else:
            print("Target file {0} present ".format(target) +
                  "and overwrite set to False. Skipping download.")
        return True

    def download_files(self, srcfilelist, tgtpath, overwrite=True,
                       ignore_error=False):
        """ Downloads list of hdfs files to local directory

        Args:
            srcfilelist(list[str]) : hdfs file to be downloaded
            tgtpath(str)           : The target directory
            overwrite(bool)        : When overwrite is set to True it will replace
                                     target file if it exists.
            ignore_error(bool)     : if set to True will ignore download and other checks.
        """

        rc = True
        for srcfile in srcfilelist:
            rc = self.download_file(srcfile, tgtpath, overwrite=overwrite,
                                    ignore_error=ignore_error) and rc
        return rc

    def upload_file(self, srcfile, tgtpath, block_size=None, replication=None,
                    permission=None, buffer_size=None, create_parent=False,
                    overwrite=True, ignore_error=False):
        """ Uploads local files to hdfs directory

        Args:
            srcfile(str)       : local source file to be uploaded.
            tgtpath(str)       : The target hdfs directory.
            block_size(int)    : Size of block for the hdfs file.
            replication(int)   : Replication factor for the file.
            permission(octal)  : File permissions to be set. Example 0755.
            buffer_size(int)   : Size of buffer used for data tranfer.
            overwrite(bool)    : True will overwrite the file if exists. Default True.
            ignore_error(bool) : If set to True will ignore failures
                                    and not raise exception.
            create_parent(bool): Creates parent folder before upload
                                    if set to True. Default is  False
        """

        if not srcfile:
            raise MissingArgumentError("Source file not provided")
        if not tgtpath:
            raise MissingArgumentError("Target Path not provided")
        if not os.path.exists(srcfile):
            raise IOError("Source file {0} does not exist".format(srcfile))

        skip = False
        open_op = "CREATE"
        filename = os.path.basename(srcfile)
        if self.is_dir(tgtpath):
            target = tgtpath + "/" + filename
        else:
            target = tgtpath
        if self.is_file(target):
            if overwrite is False:
                skip = True

        url = self._get_op_url(self._get_path_url(target), open_op)

        if create_parent is True:
            url += "&createparent=true"
        else:
            url += "&createparent=false"

        if block_size:
            url += "&blocksize=" + str(block_size)

        if replication:
            url += "&replication=" + str(replication)

        if permission:
            url += "&permission=" + str(permission)

        if buffer_size:
            url += "&buffersize=" + str(buffer_size)

        if overwrite is True:
            url += "&overwrite=true"
        else:
            url += "&overwrite=false"

        if skip is False:
            try:

                start = time.time()
                print("Uploading {0} to {1}".format(srcfile, target))
                response = Request.url_request(url, method="put", allow_redirects=False)

                if "location" in response.headers:
                    new_url = response.headers["location"]
                else:
                    err_msg = ""
                    try:
                        response_text_json = json.loads(response.text)
                        if "RemoteException" in response_text_json:
                            err_msg = response_text_json['RemoteException']['message']
                    except:
                        pass
                    raise HTTPError(err_msg)

                response = Request.url_file_upload(new_url, srcfile)

                if response.status_code != 200 and \
                                response.status_code != 201:
                    try:
                        response_text_json = json.loads(response.text)
                        if "RemoteException" in response_text_json:
                            err_msg = response_text_json['RemoteException']['message']
                    except:
                        err_msg = ""
                    raise HTTPError(err_msg)

            except HTTPError as e:
                raise IOError("File upload failed \n{0}\n"
                              .format(e.message))
            except:
                if not ignore_error:
                    raise
                else:
                    print("Upload failed".format(srcfile, target))
                    return False

            end = time.time()
            srcsize = os.path.getsize(srcfile)
            targetsize = self.get_path_status(target)["length"]
            if targetsize != srcsize:
                if not ignore_error:
                    raise IOError("source size for file {0} is {1} "
                                  .format(srcfile, srcsize) +
                                  "target size for file {0} is {1}\n"
                                  .format(target, targetsize) +
                                  "File sizes differ. Download failed.")
                else:
                    return False
            else:
                print("Total time taken to upload {0} bytes ".format(srcsize) +
                      "is {0} seconds ".format(str(end - start).strip()))
                return True

        else:
            print("Target file {0} present ".format(target) +
                  "and overwrite set to False. Skipping upload.")

    def upload_files(self, srcfilelist, tgtpath, block_size=None, replication=None,
                     permission=None, buffer_size=None, create_parent=False,
                     overwrite=True, ignore_error=False):
        """ Uploads local files to hdfs directory

        Arguments:
            srcfilelist(list)  : local source file to be uploaded.
            tgtpath(str)       : The target hdfs directory.
            block_size(int)    : Size of block for the hdfs file.
            replication(int)   : Replication factor for the file.
            permission(octal)  : File permissions to be set. Example 0755.
            buffer_size(int)   : Size of buffer used for data transfer.
            overwrite(bool)    : True will overwrite the file if exists. Default True.
            ignore_error(bool) : If set to True will ignore failures
                                    and not raise exception.
            create_parent(bool): Creates parent folder before upload
                                    if set to True. Default is  False
        """

        rc = True
        for srcfile in srcfilelist:
            rc = self.upload_file(srcfile, tgtpath, overwrite=overwrite,
                                  block_size=block_size, replication=replication,
                                  permission=permission, buffer_size=buffer_size,
                                  create_parent=create_parent, ignore_error=ignore_error) and rc
        return rc

    def upload_dir(self, srcpath, tgtpath, block_size=None, replication=None,
                   permission=None, buffer_size=None, create_parent=False,
                   overwrite=True, ignore_error=False):
        """
        Uploads all files in  local directory to HFDS.
        Note that the current implementation is not recursive and will skip directories.
        Args:
            srcpath          : local directory to be uploaded.
            tgtpath         : The target hdfs directory.
            block_size      : Size of block for the hdfs file.
            replication     : Replication factor for the file.
            permission      : File permissions to be set. Example 0755.
            buffer_size     : Size of buffer used for data transfer.
            create_parent   : Creates parent folder before upload
                                    if set to True. Default is  False
            overwrite       : True will overwrite the file if exists. Default True.
            ignore_error    : If set to True will ignore failures
                                    and not raise exception.

        Returns:
            status of the transfer - True or False
        """
        rc = True
        if os.path.isdir(srcpath):
            target_dir = os.path.basename(srcpath)
            tgtpath = tgtpath.rstrip("/") + "/" + target_dir
            if create_parent:
                self.make_dir(tgtpath, permission=permission)
            else:
                self.make_dirs(tgtpath, permission=permission)

            for srcfile in [f for f in os.listdir(srcpath) if os.isfile(os.path.join(srcpath, f))]:
                rc = self.upload_file(os.path.join(srcpath, srcfile), tgtpath, overwrite=overwrite,
                                      block_size=block_size, replication=replication,
                                      permission=permission, buffer_size=buffer_size,
                                      create_parent=create_parent, ignore_error=ignore_error) and rc
            return rc
        else:
            print("Specified Source Path {0} is not a directory.".format(srcpath))

    def upload_data_iter(self, data_iter, tgtfile, block_size=None, replication=None,
                         permission=None, buffer_size=None, create_parent=False,
                         overwrite=True, ignore_error=False):
        """ Uploads data from an iterator to hdfs directory

        Args:
            data_iter(iter)    : iterator producing data.
            tgtfile(str)       : The target hdfs file path.
            block_size(int)    : Size of block for the hdfs file.
            replication(int)   : Replication factor for the file.
            permission(octal)  : File permissions to be set. Example 0755.
            buffer_size(int)   : Size of buffer used for data tranfer.
            overwrite(bool)    : True will overwrite the file if exists. Default True.
            ignore_error(bool) : If set to True will ignore failures
                                    and not raise exception.
            create_parent(bool): Creates parent folder before upload
                                    if set to True. Default is  False
        """

        if not iter:
            raise MissingArgumentError("Iterator not provided")
        if not tgtfile:
            raise MissingArgumentError("Target Path not provided")
        skip = False
        open_op = "CREATE"
        target = tgtfile
        if self.is_file(target):
            if overwrite is False:
                skip = True

        url = self._get_op_url(self._get_path_url(target), open_op)

        if create_parent is True:
            url += "&createparent=true"
        else:
            url += "&createparent=false"

        if block_size:
            url += "&blocksize=" + str(block_size)

        if replication:
            url += "&replication=" + str(replication)

        if permission:
            url += "&permission=" + str(permission)

        if buffer_size:
            url += "&buffersize=" + str(buffer_size)

        if overwrite is True:
            url += "&overwrite=true"
        else:
            url += "&overwrite=false"

        if skip is False:
            try:

                start = time.time()
                print("Uploading to {0}".format(target))
                response = Request.url_request(url, method="put", allow_redirects=False)

                if "location" in response.headers:
                    new_url = response.headers["location"]
                else:
                    err_msg = ""
                    try:
                        response_text_json = json.loads(response.text)
                        if "RemoteException" in response_text_json:
                            err_msg = response_text_json['RemoteException']['message']
                    except:
                        pass
                    raise HTTPError(err_msg)

                response = Request.url_iter_upload(new_url, data_iter)

                if response.status_code != 200 and \
                                response.status_code != 201:
                    try:
                        response_text_json = json.loads(response.text)
                        if "RemoteException" in response_text_json:
                            err_msg = response_text_json['RemoteException']['message']
                    except:
                        err_msg = ""
                    raise HTTPError(err_msg)

            except HTTPError as e:
                raise IOError("File upload failed \n{0}\n"
                              .format(e.message))
            except:
                if not ignore_error:
                    raise
                else:
                    print("Upload failed")
                    return False

            end = time.time()
            targetsize = self.get_path_status(target)["length"]
            print("Total time taken to upload {0} bytes ".format(targetsize) +
                  "is {0} seconds ".format(str(end - start).strip()))
            return True
        else:
            print("Target file {0} present ".format(target) +
                  "and overwrite set to False. Skipping upload.")

    def append_file(self, srcfile, tgtfile, buffer_size=None, ignore_error=False):
        """ Appends local file to hdfs file

        Args:
            srcfile(str)       : Local source file to be uploaded.
            tgtfile(str)       : The hdfs target file which will be appended.
            buffer_size(int)   : Size of buffer used for data tranfer.

            ignore_error(bool) : If set to True will ignore failures
                                    and not raise exception.
        """
        if not srcfile:
            raise MissingArgumentError("Source file not provided")
        if not tgtfile:
            raise MissingArgumentError("Target File not provided")
        if not os.path.exists(srcfile):
            raise IOError("Source file {0} does not exist".format(srcfile))
        if not self.is_file(tgtfile):
            raise IOError("Target file {0} does not exist or is not a file".format(tgtfile))

        append_op = "APPEND"
        url = self._get_op_url(self._get_path_url(tgtfile), append_op)
        if buffer_size:
            url += "&buffersize=" + str(buffer_size)

        try:
            tgtfilesize = self.get_path_status(tgtfile)["length"]
            start = time.time()
            print("Appending {0} to {1}".format(srcfile, tgtfile))
            response = Request.url_request(url, method="post", allow_redirects=False)

            if "location" in response.headers:
                new_url = response.headers["location"]
            else:
                err_msg = ""
                try:
                    response_text_json = json.loads(response.text)
                    if "RemoteException" in response_text_json:
                        err_msg = response_text_json['RemoteException']['message']
                except:
                    pass
                raise HTTPError(err_msg)

            response = Request.url_file_upload(new_url, srcfile, mode="append")

            if response.status_code != 200 and response.status_code != 201:
                try:
                    response_text_json = json.loads(response.text)
                    if "RemoteException" in response_text_json:
                        err_msg = response_text_json['RemoteException']['message']
                except:
                    err_msg = ""
                raise HTTPError(err_msg)

        except HTTPError as e:
            raise IOError("File append failed \n{0}\n"
                          .format(e.message))
        except:
            if not ignore_error:
                raise
            else:
                print("Upload failed".format(srcfile, tgtfile))
                return False

        end = time.time()
        srcsize = os.path.getsize(srcfile)
        newtgtsize = self.get_path_status(tgtfile)["length"]
        if newtgtsize - tgtfilesize != srcsize:
            if not ignore_error:
                raise IOError("Amount of data that was appended" +
                              " does not not match size of source file {0} is {1} "
                              .format(srcfile, srcsize) +
                              "tgtfile size for file {0} before append is {1}\n"
                              .format(tgtfile, tgtfilesize) +
                              "tgtfile size for file {0} after append is {1}\n"
                              .format(tgtfile, tgtfilesize) +
                              "File sizes differ. Download failed.")
            else:
                return False
        else:
            print("Total time taken to append {0} bytes ".format(srcsize) +
                  "is {0} seconds ".format(str(end - start).strip()))
            return True

    def append_data_iter(self, data_iter, tgtfile, buffer_size=None, ignore_error=False):
        """ Appends data from an iterator to hdfs file

        Args:
            data_iter(iter)    : iterator producing data.
            tgtfile(str)       : The hdfs target file which will be appended.
            buffer_size(int)   : Size of buffer used for data tranfer.

            ignore_error(bool) : If set to True will ignore failures
                                    and not raise exception.
        """
        if not iter:
            raise MissingArgumentError("Iterator not provided")
        if not tgtfile:
            raise MissingArgumentError("Target Path not provided")
        if not self.is_file(tgtfile):
            raise IOError("Target file {0} does not exist or is not a file".format(tgtfile))

        append_op = "APPEND"
        url = self._get_op_url(self._get_path_url(tgtfile), append_op)
        if buffer_size:
            url += "&buffersize=" + str(buffer_size)

        try:
            tgtfilesize = self.get_path_status(tgtfile)["length"]
            start = time.time()
            print("Appending {0}".format(tgtfile))
            response = Request.url_request(url, method="post", allow_redirects=False)

            if "location" in response.headers:
                new_url = response.headers["location"]
            else:
                err_msg = ""
                try:
                    response_text_json = json.loads(response.text)
                    if "RemoteException" in response_text_json:
                        err_msg = response_text_json['RemoteException']['message']
                except:
                    pass
                raise HTTPError(err_msg)

            response = Request.url_iter_upload(new_url, data_iter, mode="append")

            if response.status_code != 200 and response.status_code != 201:
                try:
                    response_text_json = json.loads(response.text)
                    if "RemoteException" in response_text_json:
                        err_msg = response_text_json['RemoteException']['message']
                except:
                    err_msg = ""
                raise HTTPError(err_msg)

        except HTTPError as e:
            raise IOError("File append failed \n{0}\n"
                          .format(e.message))
        except:
            if not ignore_error:
                raise
            else:
                print("Upload failed")
                return False

        end = time.time()
        print("Total time taken to append" +
              "is {0} seconds ".format(str(end - start).strip()))
        return True

"""
    Provides to extract information from hcatalog through webhcat.
    Only works with hiveserver2 and an active webhcat(templeton) service.
"""

from __future__ import print_function
import getpass
from request import Request, HTTPError, url_join, url_quote
from errors import *
import json
import re

class Webhcat(object):
    """This class helps setup and manage connections to Webhcat
    and obtain information about it with the webhcat rest api calls.
    """
    def __init__(self, host=None, port=50111, protocol="http", user=None, 
                 password=None, url=None, default_database="default"):
        """
        This initializes the webhcat connection string.
        Args:
            protocol : The  protocol to be used to connect to source.
                       The default is 'http'
            host     : The host where webhcat service is running.
                       This is a required argument.
            user     : The user with which to connect to webhcat service.
            password : The password with which to connect to webhcat service.
            url    :   url which is a combination of protocol, host and port
        """
        
        if (not host or not port) and not url:
            raise ValueError("Either url or a combination of webhcat host " +
                             "port need to be provided")

        if not protocol:
            raise ValueError("protocol argument should have a value")
            
        if url:
            self.base_url = url
        else:
            self.base_url = protocol + "://" + host + ":" + str(port)
        
        self.database_ext = "templeton/v1/ddl/database"
        self.status_ext = "templeton/v1/status"
        self.version_ext = "templeton/v1/version"
        self.pig_ext = "templeton/v1/pig"
        self.hive_ext = "templeton/v1/hive"
        self.user = user or getpass.getuser()       
        self.password = password or self.user
        self.default_database = default_database
        self.http_timeout = 60

        status_response = self.url_json_request(url_join(self.base_url, self.status_ext),
                                                timeout=self.http_timeout)
        if "status" not in status_response or \
                status_response["status"] != "ok":
                raise OSError("Failed to connect to webhcat")

    

    def url_json_request(self, url, method=None, data=None, ignore_error=False,
                         headers=None, timeout = 10):
        """
        Submit a url request which returns a  json response.
        Parameters
        ---------------
        url    : Entire url
        method : None, PUT, POST, DELETE or GET
        data : Any payload that needs to be passed

        Returns
        ---------
        Response in json format
        """

        err_msg = ""
        try:
            response = Request.url_request(url, method=method, data=data, timeout=timeout, headers=headers)
            json_response = response.json()
            if "error" in json_response:
                err_msg = json_response['error']
                raise HTTPError(err_msg)
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

    def get_databases(self, pattern=None):
        """Returns the list of database names in the hive metastore.
           Args:
               pattern : The regular expression search pattern for database names.
                         This parameter is optional.
        """
        pattern = pattern or "*"
        url = url_join( self.base_url, self.database_ext) \
            + "?user.name=" + self.user  \
            + "&like=" + pattern
        return self.url_json_request(url, timeout=self.http_timeout)

    def get_database_metadata(self, database=None):
        """Returns the metadata associated to the provided hive database.
           Args:
               database : This parameter is required and represents the database
                          whose metadata information needs to be obtained.
            Returns:
                {
                 "location": <location on hdfs>,
                 "params": "{key=val}",
                 "comment": <comment>,
                 "database": <database name>
                }
        """
        database = database or self.default_database
        url = url_join( self.base_url, self.database_ext) \
            + "/" + database + "?user.name=" + self.user + "&format=extended"

        return self.url_json_request(url, timeout=self.http_timeout)

    def get_tables(self, database=None, pattern=None):
        """Returns  the list of tables in a database.
        
           Args:
               database : The name of the database. The default database.
               pattern  : The glob expression search pattern for table names.
                          This parameter is optional.
           Returns:
               list of table names under the database
        """
        pattern = pattern or "*"
        database = database or self.default_database
        url = url_join( self.base_url, self.database_ext ) \
            + "/" + database + "/table?user.name=" + self.user \
            + "&like=" + pattern

        return self.url_json_request(url, timeout=self.http_timeout)['tables']

    def get_table_metadata(self, database=None, table=None):
        """Returns the metadata associated to the provided hive table.
           Args:
               database : The name of the database.
               table  : The table name who's metadata is needed.
            Returns:
                Table details of dict format
                {
                  "partitioned": <true/false>,
                  "location": <location>
                  "outputFormat": <outputformat>
                  "columns": [
                    {
                      "name": <cname>,
                      "comment": <ccomment>,
                      "type": <ctype>
                    },
                  ],
                  "owner": "ctdean",
                  "partitionColumns": [
                    {
                      "name": <cname>,
                      "type": <ctype>
                    }
                  ],
                  "inputFormat": <input format>,
                  "database": <database name>,
                  "table": <table name>
                }
        """        
        database = database or self.default_database
        if not table:
            raise MissingArgumentError("Table name has to be provided")
        url = url_join( self.base_url, self.database_ext) \
            + "/" + database + "/table/" + table + "?user.name=" \
              + self.user + "&format=extended"
        return self.url_json_request(url, timeout=self.http_timeout)

    def create_table(self, table, columns, location=None, database=None,
                     group=None, permission=None, is_external=False,
                     comment=None, partition_by=None, cluster_by=None,
                     format=None, table_properties=None, ignore_error=False):
        """Creates a partition for a existing table.
            Args:
               table(str)          : The table name who's metadata is needed.
               columns(list[dict]) : The list of column names and types. Example shown below.
                                    [
                                     { "name": "id", "type": "bigint" },
                                     { "name": "price", "type": "float", "comment": "The unit price" }
                                    ]
               database(str)       : The name of the database.
               group(str)          : user group for partition creation.
               permission(str/int) : Permission in Octal.
               is_external(Bool)   : Bool
               comment(str)        : Comment for the table
               location(str)        : Physical location of the partition
               partition_by        : Partition column details and types as a list of dictionaries
                  (list[dict])       [{ "name": "country", "type": "string" }]
               cluster_by(dict)    : Column details for clustering. Example shown below.
                                     {
                                       "columnNames": ["id"],
                                       "sortedBy": [{ "columnName": "id", "order": "ASC"}],
                                       "numberOfBuckets": 10
                                     }
               format(dict)         : Table format details as a dictionary
                                      {
                                       "storedAs": "rcfile",
                                       "rowFormat":
                                        {
                                          "fieldsTerminatedBy": "\u0001",
                                          "serde": {
                                                   "name": "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe",
                                                   "properties": {"key": "value" }
                                                   }
                                        }
                                      }
               table_properties(dict)     : dictionary of key:value table properties.
               ignore_error               : If true, no error if the table already exists.
        """
        database = database or self.default_database
        if not table:
            raise MissingArgumentError( "Table name has to be provided")
        if not columns:
            raise MissingArgumentError( "Column/s have to be provided")
        url = url_join( self.base_url, self.database_ext ) \
              + "/" + database + "/table/" + table + \
              "?user.name=" + self.user

        if isinstance(columns, list):
            for column in columns:
                if not isinstance(column, dict):
                    raise IllegalArgumentError("Columns not of correct format" +
                                               "It should a list of dictionary objects")
        else:
            raise IllegalArgumentError( "Columns not of correct format" +
                                        "It should a list of dictionary objects")
        data = {"columns": columns}

        if location:
            data["location"] = location
        if group:
            data["group"] = group
        if permission:
            data["permissions"] = str(permission)
        if is_external is True:
            data["external"] = "true"
        if ignore_error is True:
            data["ifNotExists"] = "true"
        if comment:
            data["comment"] = comment

        if partition_by:
            if isinstance(partition_by, list):
                for column_det in partition_by:
                    if not isinstance(column_det, dict):
                        raise IllegalArgumentError("partition_by not of correct format" +
                                                   "It should a list of dictionary objects")
            else:
                raise IllegalArgumentError("partition_by not of correct format" +
                                           "It should a list of dictionary objects")
            data["partitionedBy"] = partition_by

        if cluster_by:
            if not isinstance(cluster_by, dict):
                raise IllegalArgumentError("cluster_by not of correct format" +
                                           "It should be a dictionary object")
            data["clusteredBy"] = cluster_by

        if format:
            if not isinstance(format, dict):
                raise IllegalArgumentError("format argument not of correct format" +
                                           "It should be a dictionary object")
            data["format"] = format

        if table_properties:
            raise IllegalArgumentError("table_properties argument not of correct format" +
                                       "It should be a dictionary object")
            data["table_properties"] = table_properties

        if ignore_error:
            data["ifNotExists"] = true
        data_str = json.dumps(data)
        headers={'Content-type': 'application/json'}
        return self.url_json_request(url, method="put",data=data_str,timeout=self.http_timeout, headers=headers)

        # curl -s -X PUT -HContent-type:application/json -d '{"location": "loc_a"}' \
        #      'http://localhost:50111/templeton/v1/ddl/database/default/table/test_table/partition/country=%27algeria%27?user.name=ctdean'

    def drop_table(self, database=None, table=None):
        """Drops a database table.
           Args:
               database(str) : The name of the database. Optional.
               table  : The table that needs to be dropped.
        """
        database = database or self.default_database
        if not table:
            raise MissingArgumentError("Table name has to be provided")
        url = url_join( self.base_url, self.database_ext) \
            + "/" + database + "/table/" + table + "?user.name=" \
              + self.user
        return self.url_json_request(url, method="delete", timeout=self.http_timeout)

    def rename_table(self, database=None, table=None,
                     rename=None, group=None, permission=None):
        """Renames a hive table.
           Args:
               database(str) : The name of the database. Optional.
               table  : The table that needs to be renamed.
               rename  : New name for the table.
               group  : user group.
               permission    : The permissions string to use. The format is "rwxrw-r-x"
        """
        database = database or self.default_database
        if not table:
            raise MissingArgumentError("Table name to be renamed has to be provided")
        if not rename:
            raise MissingArgumentError("New name for the table needs to be provided")
        url = url_join( self.base_url, self.database_ext) \
            + "/" + database + "/table/" + table + "?user.name=" \
              + self.user
        data = "rename=" + rename
        if group:
            data += "&group=" + group
        if permission:
            data += "&permissions=" + permission
        #data_str = json.dumps(data)
        return self.url_json_request(url, method="post", data=data, timeout=self.http_timeout)

    def copy_table(self, database=None, table=None,
                   new_table=None, group=None, permission=None,
                   location=None, is_external=False, ignore_error=False):
        """Creates a new hive table from existing table. Only metadata is copied.
           Args:
               database(str) : The name of the database. Optional.
               table  : The table that needs to be renamed.
               new_table  : New table name.
               group  : user group.
               permission    : The permissions string to use. The format is "rwxrw-r-x"
               location(str) : Physical location of the partition
               is_external(Bool)   : Bool
               ignore_error   : If true, no error if the table already exists.
        """
        database = database or self.default_database
        if not table:
            raise MissingArgumentError("Table name to be renamed has to be provided")
        if not new_table:
            raise MissingArgumentError("New name for the table needs to be provided")
        url = url_join( self.base_url, self.database_ext) \
            + "/" + database + "/table/" + table + "/like/" + new_table + "?user.name=" \
              + self.user

        data = {}
        if location:
            data["location"] = location
        if group:
            data["group"] = group
        if permission:
            data["permissions"] = str(permission)
        if is_external is True:
            data["external"] = "true"
        if ignore_error is True:
            data["ifNotExists"] = "true"
        data_str = json.dumps(data)
        headers={'Content-type': 'application/json'}
        return self.url_json_request(url, method="put",data=data_str,timeout=self.http_timeout, headers=headers)

    def add_partition(self, table, partition,
                      location, database=None,
                      group=None, permission=None,
                      ignore_error=False):
        """Creates a partition for a existing table.
           Args:
               table(str)    : The table name who's metadata is needed.
               database(str) : The name of the database.
               location(str) : Physical location of the partition
               partition(str): Name of the partition as key=value
                               value is quoted if string.
                               Example: "country='algeria'"
               group(str)    : user group for partition creation.
               permission    : The permissions string to use. The format is "rwxrw-r-x"
        """
        database = database or self.default_database
        #partition = url_quote(partition, safe="=")
        #partition = re.sub("=", "%3D", partition)
        if not table:
            raise IllegalArgumentError("Table name has to be provided")
        if not partition:
            raise IllegalArgumentError("Partition name has to be provided")
        if not location:
            raise IllegalArgumentError("location has to be provided")

        url = url_join(self.base_url, self.database_ext) \
              + "/" + database + "/table/" + table \
              + "/partition/" + partition + "?user.name=" + self.user
        data = {}
        if group:
            data["group"] = group
        if permission:
            data["permissions"] = str(permission)
        if location:
            data["location"] = location
        if ignore_error:
            data["ifNotExists"] = "true"

        headers={'Content-type': 'application/json'}
        data_str = json.dumps(data)
        return self.url_json_request(url, method="put", data=data_str, timeout=self.http_timeout, headers=headers)

    def get_partition(self, table, partition, database=None):
        """Get information about a single partition for a existing table.
           Args:
               table(str)    : The table name who's metadata is needed.
               partition(str): Name of the partition as key=value
                               value is quoted if string.
                               Example: "country='algeria'"
               database(str) : The name of the database. Optional
        """
        database = database or self.default_database
        #partition = url_quote(partition)
        if not table:
            raise IllegalArgumentError("Table name has to be provided")
        if not partition:
            raise IllegalArgumentError("Partition name has to be provided")

        url = url_join(self.base_url, self.database_ext) \
              + "/" + database + "/table/" + table \
              + "/partition/" + partition + "?user.name=" + self.user

        return self.url_json_request(url, method="get", timeout=self.http_timeout)

"""
    def execute_hive(self, statement=None, file=None, config_vars=None,
                     args=None, files=None, status_dir=None, enable_log=False,
                     callback=None):


        Args:
            statement(str)     : Hive Query language statement/s
            file(str)          : hdfs path of file containing hql statement/s
            config_vars(dict)  : dictionary of hive config variables
            args(dict)         : dictionary of hive application variables
            files(str)         : comma separated files to be copied to hdfs cluster
            status_dir(str)    : a directory where status will be written.
                                 If provided user has to delete the directory on completion.
            enable_log(str)    : If statusdir is set and enablelog is "true",
                                 collect Hadoop job configuration and logs
                                 into a directory named $statusdir/logs after the job finishes.
                                 Both completed and failed attempts are logged.
                                 The layout of subdirectories in $statusdir/logs is:
                                    logs/$job_id (directory for $job_id)
                                    logs/$job_id/job.xml.html
                                    logs/$job_id/$attempt_id (directory for $attempt_id)
                                    logs/$job_id/$attempt_id/stderr
                                    logs/$job_id/$attempt_id/stdout
                                    logs/$job_id/$attempt_id/syslog
            callback(str)      : Define a URL to be called upon job completion.
                                 You may embed a specific job ID into this URL using $jobId.
                                 This tag will be replaced in the callback URL with this job's job ID.

        Returns:
                json with id and info fields.

        url = url_join( self.base_url, self.hive_ext) \
            + "?user.name=" + self.user

        if config_vars:
            if not isinstance(config_vars, dict):
                raise IllegalArgumentError("config_vars argument is not of type dict")
            else:
                for key in config_vars.keys():
                    config_vars["define=" + key] = config_vars.pop(key)

        if config_vars:
            if not isinstance(config_vars, dict):
                raise IllegalArgumentError("config_vars argument is not of type dict")
            else:
                for key in config_vars.keys():
                    config_vars["define=" + key] = config_vars.pop(key)

        if group:
            data += "&group=" + group
        if permission:
            data += "&permissions=" + permission
"""
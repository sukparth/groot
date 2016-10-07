from __future__ import print_function
from request import Request
from util import convert_to_dict, encode_params
from constants import YARN_APP_FINAL_STATUS, YARN_APP_STATUS
from errors import *
from requests.exceptions import ConnectionError, RequestException, InvalidURL


class ResourceManager(object):
    """
    The Resource Manager REST API's can be used to get information about the cluster:
    Cluster Info, Cluster Metrics, Cluster Scheduler Information, Applications in the cluster,
    Cluster Application Statistics, Cluster Application state, Cluster Application Queues.

    """
    def __init__(self, host=None, port=8088, user=None):
        """
        Default Constructor for ResourceManager Class
        Args:
            host: Hostname of the Resource Manager Node
            port: HTTP Port of the Resource Manager
            user: Username
        """
        if host:
            self.host, self.port, = host, port
            self.url = "http://" + self.host + ":" + str(self.port)
        else:
            raise MissingParameterException("Resource Manager Host Address")

    def request(self, api_path, **query_args):
        """
        The function is used to call the url_request function by passing the url to
        connect to and optional data that needs to be passed.

        Args:
            api_path: HTTP URL with the REST API Path
            **query_args: Optional parameters that needs to be passed on to the API call
                            in the form of JSON

        Returns:
            Response object
        """
        rqst = Request()
        if query_args:
            resp = rqst.url_request(api_path, data=query_args['data'])
        else:
            resp = rqst.url_request(api_path)
        if resp.status_code == 404:
            raise InvalidURL
        if resp.status_code != 200:
            raise RequestException(resp.status_code)
        else:
            return resp

    def response_to_json(self, resp):
        """
        Function used to convert the response object to JSON format
        Args:
            resp: Response object from the REST API call

        Returns:
            JSON format of the Response Object
        """
        try:
            return resp.json()
        except Exception as e:
            print(str(e))

    def cluster_info(self):
        """
        Function used to return the overall information about the Cluster
        Returns:
            Cluster Information in JSON format
        """
        path = "/ws/v1/cluster/info"
        resp = self.request(self.url + path)
        return self.response_to_json(resp)

    def cluster_metrics(self):
        """
        Function used to return the overall metrics about the Cluster, for example,
        Number of application submitted, number of applications completed etc.
        Returns:
            Cluster Metrics in JSON format
        """
        path = "/ws/v1/cluster/metrics"
        resp = self.request(self.url + path)
        return self.response_to_json(resp)

    def cluster_scheduler(self):
        """
        Function used to return the details about the Scheduler configured in the Cluster
        Returns:
            Scheduler Information in JSON format
        """
        path = "/ws/v1/cluster/scheduler"
        resp = self.request(self.url + path)
        return self.response_to_json(resp)

    def cluster_applications(self, app_id=None, states=None, final_status=None, user=None, queue=None, limit=None,
                             started_time_begin=None, started_time_end=None, finished_time_begin=None,
                             finished_time_end=None, application_types=None, application_tags=None):
        """
        Function used to return the details of all the applications in the cluster or the list of application
        that satisfy the parameter that is being passed.

        Args:
            app_id: Application ID for which the details would be returned
            states: Comma separated list of application states in string format eg. "FAILED,KILLED"
            final_status: Final status of the application
            user: user name. Application details run by the user would be returned
            queue: queue name. Application details run in the queue will be returned
            limit: Number of applications to be returned
            started_time_begin: Start time in the form of ms since epoch
            started_time_end: End time in the form of ms since epoch
            finished_time_begin: Time by which the application would have finished in the form of ms since epoch
            finished_time_end: Time by which the application would have finshed in the form of ms since epoch
            application_types: Comma seperated list of application types eg. MAPREDUCE, TEZ, SPARK
            application_tags: Comma seperated list of application tags

        Returns:
            Application details in JSON Format

        """
        if app_id:
            path = "/ws/v1/cluster/apps/" + app_id
        else:
            path = "/ws/v1/cluster/apps"
        args = (('states', states),
                ('finalStatus', final_status),
                ('user', user),
                ('queue', queue),
                ('limit', limit),
                ('startedTimeBegin', started_time_begin),
                ('startedTimeEnd', started_time_end),
                ('finishedTimeBegin', finished_time_begin),
                ('finishedTimeEnd', finished_time_end),
                ('applicationTypes', application_types),
                ('applicationTags', application_tags))
        #
        params = convert_to_dict(args)
        valid_app_states = set(YARN_APP_STATUS)
        valid_final_app_states = set(YARN_APP_FINAL_STATUS)
        #
        if states:
            if len(states.split(',')) > 1:
                lst_states = states.split(',')
                invalid_state = [mitem for mitem in lst_states if mitem not in valid_app_states]
                if invalid_state:
                    raise InvalidParameterException(invalid_state)
            else:
                if states not in valid_app_states:
                    raise InvalidParameterException(states)
                else:
                    pass

        if final_status:
            if len(final_status.split(',')) > 1:
                lst_final_states = final_status.split(',')
                invalid_state = [mitem for mitem in lst_final_states if mitem not in valid_final_app_states]
                if invalid_state:
                    raise InvalidParameterException(invalid_state)
            else:
                if final_status not in valid_final_app_states:
                    raise InvalidParameterException(final_status)
                else:
                    pass
        try:
            resp = self.request(self.url + path, data=encode_params(params))
            return self.response_to_json(resp)
        except ConnectionError as e:
            print(str(e))

    def cluster_appstatistics(self, states=None, application_types=None):
        """
        Function to return the application statistics i.e. Number of applications grouped by types and in different
        states

        Args:
            states: comma seperated list to filter the applcations eg. "RUNNING,FINISHED"
            application_types: comma seperated list eg. "MAPREDUCE,TEZ"

        Returns:
            Application Statistics in JSON Format

        """
        path = "/ws/v1/cluster/appstatistics"
        args = (('states', states), ('applicationTypes', application_types))
        params = convert_to_dict(args)
        try:
            resp = self.request(self.url + path, data=encode_params(params))
            return self.response_to_json(resp)
        except Exception as e:
            print(str(e))

    def cluster_appattempts(self, app_id):
        """
        Function to return the details about the attempts to run the application

        Args:
            app_id: Application ID for which the details are required

        Returns:
            Details of the Application attempts in JSON Format

        """
        if app_id:
            path = "/ws/v1/cluster/apps/{0}/appattempts".format(app_id)
            resp = self.request(self.url + path)
            return self.response_to_json(resp)
        else:
            raise MissingParameterException("Application ID")

    def cluster_nodes(self, state=None, healthy=None):
        """
        Function to return the details of the node in the cluster. Details about all
        the nodes are return or those that satisfy the criteria being passed as paramters are
        returned

        Args:
            state: Comma seperated list of states eg."RUNNING"
            healthy: True or False

        Returns:
            Details of the nodes in JSON Format

        """
        path = "/ws/v1/cluster/nodes"
        args = (('state', state), ('healthy', healthy))
        params = convert_to_dict(args)
        resp = self.request(self.url + path, data=encode_params(params))
        return self.response_to_json(resp)

    def cluster_node(self, node_id=None):
        """
        Function to get information about a particular node
        Args:
            node_id: Hostname of the node for which the information is required

        Returns:
            Node information in JSON format

        """
        if node_id:
            path = "/ws/v1/cluster/nodes/{0}:{1}".format(node_id, "45454")
            resp = self.request(self.url + path)
            return self.response_to_json(resp)
        else:
            raise MissingParameterException("Node ID")

    def cluster_appstate(self, app_id=None, operation=None):
        """
        Function to get the status of the application eg. ACCEPTED
        Args:
            app_id: Application ID for which the status is requested
            operation: GET/PUT

        Returns:
            For GET operation returns the status of the Application in JSON format

        """
        if app_id and not operation:
            path = "/ws/v1/cluster/apps/{0}/state".format(app_id)
            resp = self.request(self.url + path)
            return self.response_to_json(resp)
        else:
            raise MissingParameterException("Application ID")

    def cluster_appqueue(self, app_id=None, operation=None):
        """
        Function to get the queue name in which the application is running

        Args:
            app_id: Application ID for which the queue name needs to be determined
            operation: GET/PUT

        Returns:
            For the GET Operation returns the queue in which the application is running

        """
        if app_id:
            if not operation:
                path = "/ws/v1/cluster/apps/{0}/queue".format(app_id)
                resp = self.request(self.url + path)
                return self.response_to_json(resp)
        else:
            raise MissingParameterException("Application ID")
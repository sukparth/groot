from __future__ import print_function
from request import Request
from util import convert_to_dict, encode_params
from constants import YARN_APP_FINAL_STATUS, YARN_APP_STATUS
from errors import *
from requests.exceptions import ConnectionError, RequestException, InvalidURL
import pprint


class NodeManager(object):
    """
    The Node Manager REST API's can be used to get information about the status of the node
    and information about the containers and applications running on that node.

    """
    def __init__(self, host=None, port=8042, user=None):
        """
        Default Constructor for Node Manager Class
        Args:
            host:
            port:
            user:
        """
        if host:
            self.host, self.port, = host, port
            self.url = "http://" + self.host + ":" + str(self.port)
        else:
            raise MissingParameterException("Node Manager Host Address")

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
            print("Status")
            print(resp.raise_for_status())
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

    def node_info(self):
        """
        Function used to return the overall information about the Node
        Returns:
            Node Information in JSON format
        """
        path = "/ws/v1/node"
        resp = self.request(self.url + path)
        return self.response_to_json(resp)

    def node_apps(self, state=None, user=None):
        """
        Function used to return details about running apps
        Args:
            state: state of the application
            user: user running the application

        Returns:
            JSON format of collection of application objects

        """
        path = "/ws/v1/node/apps"
        args = (('state', state), ('user', user))
        params = convert_to_dict(args)
        print(params)
        try:
            resp = self.request(self.url + path, data=encode_params(params))
            return self.response_to_json(resp)
        except Exception as e:
            print(str(e))

    def node_app(self, app_id=None):
        """
        Function used to return details about application running or run in this node manager
        Args:
            app_id: Application ID running on the node being
                    requested

        Returns:
            JSON format of the application object details

        """
        if app_id:
            path = "/ws/v1/node/apps/{0}".format(app_id)
            resp = self.request(self.url + path)
            return self.response_to_json(resp)
        else:
            raise MissingParameterException("Application ID")

    def node_containers(self):
        """
        Function used to return a collection of container objects

        Returns:
            JSON format of details about collection of container objects
        """
        path = "/ws/v1/node/containers"
        try:
            resp = self.request(self.url + path)
            return self.response_to_json(resp)
        except Exception as e:
            print(str(e))

    def node_container(self, container_id=None):
        """
        Function used to return details about container running in this node manager
        Args:
            container_id: Container ID running on the node being requested

        Returns:
            JSON format of the container object details

        """
        if container_id:
            path = "/ws/v1/node/containers/{0}".format(container_id)
            resp = self.request(self.url + path)
            return self.response_to_json(resp)
        else:
            raise MissingParameterException("Container ID")
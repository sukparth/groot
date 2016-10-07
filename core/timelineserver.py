from __future__ import print_function
from request import Request
from util import convert_to_dict, encode_params
from constants import YARN_APP_FINAL_STATUS, YARN_APP_STATUS
from errors import *
from requests.exceptions import ConnectionError, RequestException, InvalidURL
import pprint


class TimelineServer(object):
    """
    The Timeline Server REST API's can be used to get information about the status of applications
    and information about the containers and applications that ran on the cluster.

    Timeline Server is enabled in HDP version 2.4

    """
    def __init__(self, host=None, port=8188, user=None):
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

    def timeline_info(self):
        """
        Function used to return the overall information about the Node
        Returns:
            Node Information in JSON format
        """
        path = "/ws/v1/timeline"
        resp = self.request(self.url + path)
        return self.response_to_json(resp)

    def timeline_tez_dag_id(self, dag_id=None):
        if not dag_id:
            path = "/ws/v1/timeline/TEZ_DAG_ID/"
        else:
            path = "/ws/v1/timeline/TEZ_DAG_ID/" + dag_id
        resp = self.request(self.url + path)
        return self.response_to_json(resp)

    def timeline_tez_app(self, app_id=None, user=None, limit=None):
        if app_id:
            path = "/ws/v1/timeline/TEZ_DAG_ID/?primaryFilter=applicationId:{0}&limit={1}".format(app_id, limit)
        elif user:
            path = "/ws/v1/timeline/TEZ_DAG_ID/?primaryFilter=user:{0}&limit={1}".format(user, limit)
        resp = self.request(self.url + path)
        return self.response_to_json(resp)

    def timeline_apps(self, app_id=None):
        if app_id:
            path = "/ws/v1/applicationhistory/apps/{0}".format(app_id)
        else:
            path = "/ws/v1/applicationhistory/apps"
        resp = self.request(self.url + path)
        return self.response_to_json(resp)

    def timeline_app_attempts(self, app_id=None, attempt_id=None):
        if app_id and attempt_id:
            path = "/ws/v1/applicationhistory/apps/{0}/appattempts/{1}".format(app_id, attempt_id)
            resp = self.request(self.url + path)
            return self.response_to_json(resp)
        elif app_id and not attempt_id:
            path = "/ws/v1/applicationhistory/apps/{0}/appattempts".format(app_id)
            resp = self.request(self.url + path)
            return self.response_to_json(resp)
        else:
            raise MissingParameterException("Application ID or App Attempt ID")

    def timeline_app_containers(self, app_id=None, attempt_id=None, container_id=None):
        if app_id and attempt_id and container_id:
            path = "/ws/v1/applicationhistory/apps/{0}/appattempts/{1}/containers/{2}".format(app_id, attempt_id, container_id)
            resp = self.request(self.url + path)
            return self.response_to_json(resp)
        elif app_id and attempt_id and not container_id:
            path = "/ws/v1/applicationhistory/apps/{0}/appattempts/{1}/containers".format(app_id, attempt_id)
            resp = self.request(self.url + path)
            return self.response_to_json(resp)
        else:
            raise MissingParameterException("Application ID or App Attempt ID or Container ID")


def main():
    obj = TimelineServer(host="hdp0b.internal.shutterfly.com")
    #pprint.pprint(obj.timeline_info())
    pprint.pprint(obj.timeline_apps())
    #pprint.pprint(obj.timeline_tez_dag_id(dag_id='dag_1468873272196_0246_1'))
    #pprint.pprint(obj.timeline_tez_app(app_id='application_1468873272196_0249'))
    #pprint.pprint(obj.timeline_tez_app(user='dwuser', limit=2))
    #pprint.pprint(obj.timeline_apps(app_id='application_1466141321303_1591'))
    #pprint.pprint(obj.timeline_app_attempts(app_id='application_1466141321303_1591',attempt_id='appattempt_1466141321303_1591_000001'))
    #pprint.pprint(obj.timeline_app_containers(app_id='application_1466141321303_1591', attempt_id='appattempt_1466141321303_1591_000001', container_id='container_1466141321303_1591_01_000012'))

if __name__ == "__main__":
    main()


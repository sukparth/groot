from __future__ import print_function
from request import Request
from util import convert_to_dict, encode_params
from constants import MAPRED_APP_STATUS
from errors import *
from requests.exceptions import ConnectionError, RequestException, InvalidURL


class HistoryServer(object):
    """
    The History Server REST API's can be used to get information about the status of the node
    and information about the containers and applications running on that node.

    """
    def __init__(self, host=None, port=19888, user=None):
        """
        Default Constructor for History Server Class
        Args:
            host:
            port:
            user:
        """
        if host:
            self.host, self.port, = host, port
            self.url = "http://" + self.host + ":" + str(self.port)
        else:
            raise MissingParameterException("History Server Host Address")

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

    def historyserver_info(self):
        """
        Function used to return the overall information about the History Server
        Returns:
            History Server Information in JSON format
        """
        path = "/ws/v1/history"
        resp = self.request(self.url + path)
        return self.response_to_json(resp)

    def mapreduce_jobs(self, user=None, state=None, queue=None, limit=None,
                       started_time_begin=None, started_time_end=None, finished_time_begin=None,
                       finished_time_end=None):
        """

        Args:
            state: Comma separated list of application states in string format eg. "FAILED,KILLED"
            user: user name. Application details run by the user would be returned
            queue: queue name. Application details run in the queue will be returned
            limit: Number of applications to be returned
            started_time_begin: Start time in the form of ms since epoch
            started_time_end: End time in the form of ms since epoch
            finished_time_begin: Time by which the application would have finished in the form of ms since epoch
            finished_time_end: Time by which the application would have finshed in the form of ms since epoch

        Returns:

        """
        path = "/ws/v1/history/mapreduce/jobs"
        args = (('state', state),
                ('user', user),
                ('queue', queue),
                ('limit', limit),
                ('startedTimeBegin', started_time_begin),
                ('startedTimeEnd', started_time_end),
                ('finishedTimeBegin', finished_time_begin),
                ('finishedTimeEnd', finished_time_end))

        #
        params = convert_to_dict(args)
        valid_app_states = set(MAPRED_APP_STATUS)
        #
        if state:
            if len(state.split(',')) > 1:
                lst_states = state.split(',')
                invalid_state = [mitem for mitem in lst_states if mitem not in valid_app_states]
                if invalid_state:
                    raise InvalidParameterException(invalid_state)
            else:
                if state not in valid_app_states:
                    raise InvalidParameterException(state)
                else:
                    pass

        try:
            resp = self.request(self.url + path, data=encode_params(params))
            return self.response_to_json(resp)
        except ConnectionError as e:
            print(str(e))

    def mapreduce_job(self, job_id=None):
        """
        Function used to return details about application running or run in this node manager
        Args:
            job_id: Job ID of particular job

        Returns:
            JSON format of the job details

        """
        if job_id:
            path = "/ws/v1/history/mapreduce/jobs/{0}".format(job_id)
            resp = self.request(self.url + path)
            return self.response_to_json(resp)
        else:
            raise MissingParameterException("Job ID")

    def mapreduce_job_attempts(self, job_id=None):
        """
        Function used to return details about collection of job attempt objects for the JOB ID
        Args:
            job_id: Job ID of particular job

        Returns:
            JSON format of the job attempt details

        """
        if job_id:
            path = "/ws/v1/history/mapreduce/jobs/{0}/jobattempts".format(job_id)
            resp = self.request(self.url + path)
            return self.response_to_json(resp)
        else:
            raise MissingParameterException("Application ID")

    def mapreduce_job_counters(self, job_id=None):
        """
        Function used to return details about collection of resources that represe the job counters for the JOB ID
        Args:
            job_id: Job ID of particular job

        Returns:
            JSON format of the job counter details

        """
        if job_id:
            path = "/ws/v1/history/mapreduce/jobs/{0}/counters".format(job_id)
            resp = self.request(self.url + path)
            return self.response_to_json(resp)
        else:
            raise MissingParameterException("Job ID")

    def mapreduce_job_conf(self, job_id=None):
        """
        Function used to return details about job configuration for the JOB ID
        Args:
            job_id: Job ID of particular job

        Returns:
            JSON format of the job configuration details

        """
        if job_id:
            path = "/ws/v1/history/mapreduce/jobs/{0}/conf".format(job_id)
            resp = self.request(self.url + path)
            return self.response_to_json(resp)
        else:
            raise MissingParameterException("Job ID")

    def mapreduce_job_tasks(self, job_id=None):
        """
        Function used to return details about tasks for the job represented by the JOB ID
        Args:
            job_id: Job ID of particular job

        Returns:
            JSON format of the task details for the particular job

        """
        if job_id:
            path = "/ws/v1/history/mapreduce/jobs/{0}/tasks".format(job_id)
            resp = self.request(self.url + path)
            return self.response_to_json(resp)
        else:
            raise MissingParameterException("Job ID")

    def mapreduce_job_task(self, job_id=None, task_id=None):
        """
        Function used to return details about a particular task for the job represented by the JOB ID
        Args:
            job_id: Job ID of particular job

        Returns:
            JSON format of the detail of the particular task for the job

        """
        if job_id:
            if task_id:
                path = "/ws/v1/history/mapreduce/jobs/{0}/tasks/{1}".format(job_id, task_id)
                resp = self.request(self.url + path)
                return self.response_to_json(resp)
            else:
                raise MissingParameterException("Task ID")
        else:
            raise MissingParameterException("Job ID")

    def mapreduce_job_task_counters(self, job_id=None, task_id=None):
        """
        Function used to return details about a particular task counters for the job represented by the JOB ID
        Args:
            task_id: ID of the task
            job_id: Job ID of particular job

        Returns:
            JSON format of the counter details of the particular task for the job

        """
        if job_id:
            if task_id:
                path = "/ws/v1/history/mapreduce/jobs/{0}/tasks/{1}/counters".format(job_id, task_id)
                resp = self.request(self.url + path)
                return self.response_to_json(resp)
            else:
                raise MissingParameterException("Task ID")
        else:
            raise MissingParameterException("Job ID")

    def mapreduce_job_task_attempts(self, job_id=None, task_id=None):
        """
        Function used to return details about a particular task attempts for the job represented by the JOB ID
        Args:
            task_id: Task ID of the task
            job_id: Job ID of particular job

        Returns:
            JSON format of the attempt details of the particular task for the job

        """
        if job_id:
            if task_id:
                path = "/ws/v1/history/mapreduce/jobs/{0}/tasks/{1}/attempts".format(job_id, task_id)
                resp = self.request(self.url + path)
                return self.response_to_json(resp)
            else:
                raise MissingParameterException("Task ID")
        else:
            raise MissingParameterException("Job ID")

    def mapreduce_job_task_attempt(self, job_id=None, task_id=None, attempt_id=None):
        """
        Function used to return details about a particular task attempts for the job represented by the JOB ID
        Args:
            task_id: Task ID
            attempt_id: Task attempt ID
            job_id: Job ID of particular job

        Returns:
            JSON format of the attempt details of the particular task for the job

        """
        if job_id:
            if task_id:
                if attempt_id:
                    path = "/ws/v1/history/mapreduce/jobs/{0}/tasks/{1}/attempts/{2}".format(job_id, task_id, attempt_id)
                    resp = self.request(self.url + path)
                    return self.response_to_json(resp)
                else:
                    raise MissingParameterException("Attempt ID")
            else:
                raise MissingParameterException("Task ID")
        else:
            raise MissingParameterException("Job ID")

    def mapreduce_job_task_attempt_counters(self, job_id=None, task_id=None, attempt_id=None):
        """
        Function used to return details about a particular task attempts for the job represented by the JOB ID
        Args:
            task_id: Task ID
            attempt_id: Attempt ID
            job_id: Job ID of particular job

        Returns:
            JSON format of the attempt details of the particular task for the job

        """
        if job_id:
            if task_id:
                if attempt_id:
                    path = "/ws/v1/history/mapreduce/jobs/{0}/tasks/{1}/attempts/{2}/counters".format(job_id, task_id, attempt_id)
                    resp = self.request(self.url + path)
                    return self.response_to_json(resp)
                else:
                    raise MissingParameterException("Attempt ID")
            else:
                raise MissingParameterException("Task ID")
        else:
            raise MissingParameterException("Job ID")
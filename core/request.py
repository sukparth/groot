try:
    from urlparse import urljoin as url_join
    from urllib import quote as url_quote
    from urllib import urlretrieve
except ImportError:
    from urllib.request import urlretrieve
    from urllib.parse import urljoin as url_join
    from urllib.parse import quote as url_quote

import requests
import os
from errors import *


class Request(object):
    """ Requests class for rest calls to hadoop web rest api's"""

    @staticmethod
    def url_request(url, method=None, data=None,
                    allow_redirects=False, timeout=10,
                    stream=False, headers=None):
        """
        Submit a url request and get a response.
        Args:
            url(str)    : Entire url
            method(str) : None, PUT, POST, DELETE or GET
            data(str)   : Any payload that needs to be passed
            allow_redirects : Default False
            headers       : Header for the request as a json string

        Returns:
            Response in format
        """
        if not method or method.lower() == "get":
            return requests.get(url, allow_redirects=allow_redirects,
                                timeout=timeout, params=data,
                                stream=stream, headers=headers)
        elif method.lower() == "put":
            if data:
                return requests.put(url, data=data,
                                    allow_redirects=allow_redirects,
                                    timeout=timeout, headers=headers)
            else:
                return requests.put(url,
                                    allow_redirects=allow_redirects,
                                    timeout=timeout, headers=headers)
        elif method.lower() == "post":
            if data:
                return requests.post(url, data=data,
                                    allow_redirects=allow_redirects,
                                    timeout=timeout, headers=headers)
            else:
                return requests.post(url,
                                    allow_redirects=allow_redirects,
                                    timeout=timeout, headers=headers)
        elif method.lower() in ["delete", "del"]:
            return requests.delete(url, data=data,
                                   allow_redirects=allow_redirects,
                                   timeout=timeout, headers=headers)
        else:
            raise ValueError("Unrecognized method {}".format(method))

    @staticmethod
    def url_file_download(url, tgtpath):
        """
        Get url file into local target.
        This method is blocking.
        Args:
            url    : Entire url
            tgtpath : Target local or network mounted file path
        """
        response = urlretrieve(url, tgtpath)
        return response

    @staticmethod
    def url_file_upload(url, srcfile, mode="create"):
        """
        Put source file as url.
        This method is blocking.
        Args:
            url    : Entire url
            srcfile : Source file path
            mode   : "create" or "append"
        """
        if mode not in ("create", "append"):
            raise IllegalArgumentError("mode should have value 'create' or 'append'" +
                                       "provided value {0}".format(mode))
        if mode == "create":
            response = requests.put(url, data=open(srcfile, "rb"))
        else:
            response = requests.post(url, data=open(srcfile, "rb"))
        return response

    @staticmethod
    def url_iter_upload(url, data_iter, mode="create"):
        """
        Uploads iterator data output to a url.
        This method is blocking.
        Args:
            url  : Entire url
            data_iter : generator function or object of type generator
            mode   : "create" or "append"
        """

        import inspect
        import types
        from collections import Iterable
        if mode not in ("create", "append"):
            raise IllegalArgumentError("mode should have value 'create' or 'append'" +
                                       "provided value {0}".format(mode))
        if not inspect.isgeneratorfunction(data_iter) \
                and not isinstance(data_iter, types.GeneratorType) and not isinstance(data_iter, Iterable):
            raise IllegalArgumentError("Argument is not a iterator or generator function or of generator type")
        if mode == "create":
            response = requests.put(url, data=data_iter)
        else:
            response = requests.post(url, data=data_iter)
        return response


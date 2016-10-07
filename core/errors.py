import requests.exceptions as reqex

class InvalidParameterException(Exception):
    def __init__(self, args):
        self.message = "Invalid Parameter(s) passed in the request"
        if type(args) is list:
            self.parameter = " ".join(args)
        else:
            self.parameter = "".join(args)


class MissingParameterException(Exception):
    def __init__(self, args):
        self.message = "Missing Parameter"
        if type(args) is list:
            self.parameter = " ".join(args)
        else:
            self.parameter = "".join(args)


# class RequestException(Exception):
#     def __init__(self, args):
#         self.message = "Call to REST API unsuccessful"
#         self.parameter = args


class IllegalArgumentError(ValueError):
    def __init__(self, args):
        self.message = "Invalid Argument "
        if isinstance(args, list):
            self.message += " ".join(args)
        else:
            self.message += "".join(args)
        super(IllegalArgumentError, self).__init__(self.message)


class MissingArgumentError(ValueError):
    def __init__(self, args):
        self.message = "Missing Argument "
        if isinstance(args, list):
            self.message += " ".join(args)
        else:
            self.message += "".join(args)
        super(MissingArgumentError, self).__init__(self.message)


class RequestError(Exception):
    def __init__(self, args):
        self.message = "Call to REST API unsuccessful"
        if isinstance(args, list):
            self.message += " ".join(args)
        else:
            self.message += "".join(args)
        super(RequestError, self).__init__(self.message)


class HTTPError(reqex.HTTPError):
    """ An exception occured"""
    pass



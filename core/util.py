try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode

from fnmatch import fnmatch
import re

def convert_to_dict(args=None):
    if args:
        params = dict((key, value) for key, value in args if value is not None)
        return params


def encode_params(params=None):
    if params:
        return urlencode(params)


def regex_search(text, pattern=None):
    pattern = pattern or ".*"
    return re.search( pattern, text )


def glob_search(text, pattern=None):
    pattern = pattern or "*"
    return fnmatch( text, pattern )


class ReadOnlyClass( type ):
    def __setattr__(cls, name, value):
        raise AttributeError( 'Cannot set class attribute.' )


def timer(func):
    def outfunc(*args, **kwargs):
        import time
        ___start___ = time.time( )
        print("The function is {0}".format( func.__name__ ))
        print("The start time is {0}".format( time.ctime( int( ___start___ ) ) ))
        func( *args, **kwargs )
        ___end___ = time.time( )
        print("The end time is {0}".format( time.ctime( int( ___end___ ) ) ))
        print("Total time taken is {0}".format( ___end___ - ___start___ ))

    return outfunc


def merge_dict(dict1, dict2):
    """ Merges 2 dictionaries. If there are common keys in both,
        the value in the second dictionary will have precedence"""
    return dict( (key, value) for (key, value) in (dict1.items( ) + dict2.items( )) )

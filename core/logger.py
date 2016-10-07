"""
    Provides a standard setup for logging messages
    when using python. All messages will be of the below format
    by default.
    <time in YYYYMMDD HH:mm:SS.FFF>|<host>|<script>|<loglevel>|<message>
    Loglevels WARNING and above will be sent to standard error    
"""

import logging
import  sys
import socket
import os
import traceback




class stdoutFilter(logging.Filter,object):
    """ 
    Filter on Log level applied to stdout.
    returns 1 when less or equal to provided maxlevel
    """
    def __init__(self, maxLevel, name =""):
        super(stdoutFilter, self).__init__(name)
        self.maxLevel = maxLevel

    def filter(self, record):
        #non-zero return indicates that the message should be logged
        return 1 if record.levelno <= self.maxLevel else 0

class DPLogConstants:
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING =  logging.WARNING
    ERROR =  logging.ERROR
    CRITICAL = logging.CRITICAL

class DPLogger(object):
    """
    Sets up a simple logging facility.
    Messages with levels NOSET to INFO go to stdout
    Messages with levels WARN and above go to stderr
    """
    def __init__(self, script=__name__ , logLevel=logging.INFO, logDelimiter="|"):
        # Constructor  
        try:
            hostname = socket.gethostname()
        except:
            hostname = "unknown"
        
        # Check to see environment ENV_LOG_MESSAGE is set to true for extended formatting
        envLogFlag = False
        try:
            if os.environ['ENV_LOG_MESSAGE'] in [ 'True','true','yes','Yes','y','Y','0' ]:
                envLogFlag = True
        except:
            pass

        try:
            if os.environ['_DEBUG_MODE'] == "1":
                logLevel = DPLogConstants.DEBUG
        except:
            pass

        try:
            if os.environ['_DP_LOG_LEVEL'] in [ DPLogConstants.DEBUG,
                                                DPLogConstants.CRITICAL,
                                                DPLogConstants.INFO,
                                                DPLogConstants.ERROR,
                                                DPLogConstants.WARNING]:
                logLevel = os.environ['_DP_LOG_LEVEL']
        except:
            pass

        if envLogFlag:
            logFormat = logging.Formatter(fmt = '%(asctime)s.%(msecs)06d' +
                                                logDelimiter + hostname +
                                                logDelimiter + script +
                                                logDelimiter + '%(levelname)s' +
                                                logDelimiter + '%(message)s' ,
                                                datefmt='%Y-%m-%d %H:%M:%S')
        else:
            logFormat = logging.Formatter('%(levelname)s' + ': '  + '%(message)s')
        
        # Set logger ( root level) with formatting
        self.logger = logging.getLogger(script)
        self.logger.setLevel(logLevel)
        
 
        # Create handler for standard output with maximum log level as INFO
        stdoutHandler = logging.StreamHandler(sys.stdout)
        stdoutHandler.setLevel(logLevel)
        stdoutHandler.addFilter(stdoutFilter(logging.INFO))
        stdoutHandler.setFormatter(logFormat)
        self.logger.addHandler(stdoutHandler)

        # Create handler for standard error with minimum log level as WARNING
        stderrHandler = logging.StreamHandler(sys.stderr)
        stderrHandler.setLevel(logging.WARNING)
        stderrHandler.setFormatter(logFormat)
        self.logger.addHandler(stderrHandler)
   
        def errtrace(tr):
            exc_type, exc_value, exc_traceback = tr
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                                      limit=5, file=sys.stderr)

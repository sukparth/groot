"""
    Reads the project configuration file groot.ini
"""

import os
from ConfigParser import SafeConfigParser

class DPConfig(object):
    """This class is used the initialize project configuration
       attributes and methods.
    """
    _DEFUALT_CONF_DIR = "conf"
    _DEFAULT_CONF_FILE = "groot.ini"

    @property
    def DEFAULT_CONF_DIR(self):
        return self._DEFAULT_CONF_DIR
    @property
    def DEFAULT_CONF_FILE(self):
        return self._DEFAULT_CONF_FILE
        
    def __init__(self,config = None):
        conf_file = config or os.path.join(
        os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
                                        self.DEFAULT_CONF_DIR, self.DEFAULT_CONF_FILE)
        self.config = SafeConfigParser().read(conf_file)
       


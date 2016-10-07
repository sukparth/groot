# -*- coding: utf-8 -*-
"""
Created on Wed Jan 12 18:21:26 2016

@author: psukumar
"""

import sys
import os
# use PYTHONPATH to setup path
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../core")
from webhcat import Webhcat
import config as tconfig


def main():
    web_hcat_port = tconfig.webhcat["port"] or 50111
    web_hcat_host = tconfig.webhcat["host"] or "localhost"
    whcat = Webhcat(host=web_hcat_host, port=web_hcat_port,
                    default_database="firehose", user="hive")
    print(whcat.get_tables("firehose"))
    print whcat.get_table_metadata(database="firehose",
                                   table="statisticsevent_prod")

if __name__ == "__main__":
    main()
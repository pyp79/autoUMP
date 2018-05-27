#!/usr/bin/python3
# -*- coding:utf-8 -*-
#Author: Pang Yapeng

import re
import sys
import logging
import pymysql
import os
import configparser

cur_path = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(cur_path, '../conf/config.ini')
conf = configparser.ConfigParser()
conf.read(config_path, encoding='utf-8-sig')

logfile = conf.get("SOURCE_FILE", "TEST")
print(logfile)


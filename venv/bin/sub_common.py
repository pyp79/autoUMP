#!/usr/bin/python
# -*- coding: UTF-8 -*-

import re
import sys
import logging
import pymysql
import os
import configparser

def get_conf():
# read config
    cur_path=os.path.dirname(os.path.realpath(__file__))
    config_path=os.path.join(cur_path,'../conf/config.ini')
    conf = configparser.ConfigParser()
    conf.read(config_path,encoding='utf-8-sig')
    return conf

def get_conn():
    conf = get_conf()
    DB_IP = conf.get("MYSQL", "DB_IP")
    DB_PORT = conf.get("MYSQL", "DB_PORT")
    DB_USER = conf.get("MYSQL", "DB_USER")
    DB_PASSWORD = conf.get("MYSQL", "DB_PASSWORD")
    DB_SCHEMA = conf.get("MYSQL", "DB_SCHEMA")
    DB_CHARSET = conf.get("MYSQL", "DB_CHARSET")
    conn = pymysql.connect(host=DB_IP,port=int(DB_PORT),user=DB_USER,passwd=DB_PASSWORD,db=DB_SCHEMA,use_unicode=True,charset=DB_CHARSET)
    return(conn)

def clean_db(sql):
    conn = get_conn()
    cursor = conn.cursor();
    cursor.execute(sql);
    conn.commit();

def get_logger(FILENAME):
    conf = get_conf()
    LOG_FILE =os.path.join(conf.get("BASE_CONF", "LOG_PATH") , FILENAME)

    # 设置log
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(LOG_FILE,encoding="UTF-8")
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def isIP(str): #判断字符串是否IP地址格式
    p = re.compile('^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$')
    if p.match(str):
        return True
    else:
        return False
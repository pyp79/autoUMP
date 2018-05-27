#!/usr/bin/python3
# -*- coding:utf-8 -*-
#Author: Pang Yapeng

import datetime
import time
import re
import csv
from sub_common import *

# 设置log
logger = get_logger("import_auto.log")

# 特定参数
conf = get_conf()
PROCESS_INFO = conf.get("SOURCE_FILE", "AMDB_PROCESS_INFO")
IP_INFO = conf.get("SOURCE_FILE", "AMDB_IP_INFO")
PROCESS_TABLE = "AUTO_AMDB_PROCESS_RAW"
IP_TABLE = "AUTO_AMDB_IP_RAW"

# 设置全局变量
write_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
version = datetime.datetime.now().strftime('%Y%m%d')
global counter_process
global counter_ip

def main():
    global counter_process
    global counter_ip
    counter_process = 0
    counter_ip = 0

    logger.info("Begin to insert to table:%s,%s",PROCESS_TABLE,IP_TABLE)

    conn = get_conn()
    cursor = conn.cursor()

    sql = "delete from {0} where VERSION = %s".format(PROCESS_TABLE)
    cursor.execute(sql,version)
    sql = "delete from {0} where VERSION = %s".format(IP_TABLE)
    cursor.execute(sql,version)

    v_date_para = re.findall(r"\[(.*)\]",PROCESS_INFO)[0]
    new_date = time.strftime(v_date_para)
    PROCESS_INFO_NEW = re.sub("\[.*\]",new_date,PROCESS_INFO)
    with open(PROCESS_INFO_NEW, encoding="UTF-8") as csvfile:
        data = csv.DictReader(csvfile)
        for row in data:
            sql = '''insert into {0} (VERSION,WRITE_TIME,SERVICE_ID,APP_CODE,IP_ADDRESS,PROCESS_USER,
                PROCESS_COMMAND,PROCESS_DESC,PROCESS_NUM,IS_LOADBALANCE,HOST_ROLE,USERS_IN_BANK,USERS_TOTAL,USERS_ROLE,
                USERS_TYPE,PROCESS_ALIAS,MIN_COUNT,MAX_COUNT,BEGIN_TIME,END_TIME
                ) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''.format(PROCESS_TABLE)
            para = (version,write_time,row['SERVICE_ID'],row['APP_CODE'],row['IP_ADDRESS'],row['PROCESS_USER'], row['PROCESS_COMMAND'],row['PROCESS_DESC'],row['PROCESS_NUM'],row['IS_LOADBALANCE'], \
                                 row['HOST_ROLE'],row['USERS_IN_BANK'],row['USERS_TOTAL'],row['USERS_ROLE'],row['USERS_TYPE'], \
                                 row['PROCESS_ALIAS'],row['MIN_COUNT'],row['MAX_COUNT'],row['BEGIN_TIME'],row['END_TIME'])
            cursor.execute(sql,para)
            counter_process += 1

    v_date_para = re.findall(r"\[(.*)\]",IP_INFO)[0]
    new_date = time.strftime(v_date_para)
    IP_INFO_NEW = re.sub("\[.*\]",new_date,IP_INFO)
    with open(IP_INFO_NEW, encoding="UTF-8") as csvfile:
        data = csv.DictReader(csvfile)
        for row in data:
            sql = '''insert into {0} (VERSION,WRITE_TIME,APP_CODE,IP_ADDRESS,SERVICE_IP,IP_TYPE) 
                values(%s,%s,%s,%s,%s,%s)'''.format(IP_TABLE)
            if row['IP_ADDRESS'] == row['SERVICE_IP']:
                ip_type = 'VIP'
            else:
                ip_type = 'HOST_IP'
            para = (version,write_time,row['APP_CODE'],row['IP_ADDRESS'],row['SERVICE_IP'], ip_type)
            cursor.execute(sql,para)
            counter_ip += 1

    conn.commit()
    conn.close()
    logger.info("Success insert records:%s,%s", counter_process,counter_ip)

if __name__ == '__main__':
        main()


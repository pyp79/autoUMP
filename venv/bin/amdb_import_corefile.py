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
COREFILE_INFO = conf.get("SOURCE_FILE", "AMDB_COREFILE_INFO")
COREFILE_TABLE = "AUTO_AMDB_COREFILE_RAW"

# 设置全局变量
write_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
version = datetime.datetime.now().strftime('%Y%m%d')
global counter

def main():
    global counter
    counter = 0

    logger.info("Begin to insert to table:%s",COREFILE_TABLE)

    conn = get_conn()
    cursor = conn.cursor()

    sql = "delete from {0} where VERSION = %s".format(COREFILE_TABLE)
    cursor.execute(sql,version)

    v_date_para = re.findall(r"\[(.*)\]",COREFILE_INFO)[0]
    new_date = time.strftime(v_date_para)
    COREFILE_INFO_NEW = re.sub("\[.*\]",new_date,COREFILE_INFO)
    with open(COREFILE_INFO_NEW, encoding="UTF-8") as csvfile:
        data = csv.DictReader(csvfile)
        for row in data:
            sql = '''insert into {0} (VERSION,WRITE_TIME,APPSYSCD,IP,APP_USER,COREFILEDIR)
                  values(%s,%s,%s,%s,%s,%s)'''.format(COREFILE_TABLE)
            para = (version,write_time,row['APPSYSCD'],row['IP'],row['APP_USER'],row['COREFILEDIR'])
            cursor.execute(sql,para)
            counter += 1

    conn.commit()
    conn.close()
    logger.info("Success insert records:%s", counter)

if __name__ == '__main__':
        main()


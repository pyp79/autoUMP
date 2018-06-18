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
PORT_INFO = conf.get("SOURCE_FILE", "AMDB_PORT_INFO")
PORT_TABLE = "AUTO_AMDB_PORT_RAW"

# 设置全局变量
write_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
version = datetime.datetime.now().strftime('%Y%m%d')
global counter

def main():
    global counter
    counter = 0

    logger.info("Begin to insert to table:%s",PORT_TABLE)

    conn = get_conn()
    cursor = conn.cursor()

    sql = "delete from {0} where VERSION = %s".format(PORT_TABLE)
    cursor.execute(sql,version)

    v_date_para = re.findall(r"\[(.*)\]",PORT_INFO)[0]
    new_date = time.strftime(v_date_para)
    PORT_INFO_NEW = re.sub("\[.*\]",new_date,PORT_INFO)
    with open(PORT_INFO_NEW, encoding="UTF-8") as csvfile:
        data = csv.DictReader(csvfile)
        for row in data:
            sql = '''insert into {0} (VERSION,WRITE_TIME,APPSYSCD,FLOATIP,PORT_TYPE,START_PORT,END_PORT)
                  values(%s,%s,%s,%s,%s,%s,%s)'''.format(PORT_TABLE)
            para = (version,write_time,row['APPSYSCD'],row['FLOATIP'],row['PORT_TYPE'],row['START_PORT'],row['END_PORT'])
            cursor.execute(sql,para)
            counter += 1

    conn.commit()
    conn.close()
    logger.info("Success insert records:%s", counter)

if __name__ == '__main__':
        main()


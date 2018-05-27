#!/usr/bin/python3
# -*- coding:utf-8 -*-
#Author: Pang Yapeng

import datetime
from sub_common import *

# 设置log
logger = get_logger("process.log")

# 特定参数
PROCESS_CONF_TABLE = "AUTO_PROCESS_CONF"
PROCESS_TABLE = "AUTO_AMDB_PROCESS_CONF"

# 设置全局变量
write_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
version = datetime.datetime.now().strftime('%Y%m%d')
global counter

def main():
    global counter
    counter = 0

    logger.info("Begin to insert to table:%s",PROCESS_CONF_TABLE)

    conn = get_conn()
    cursor = conn.cursor()

    sql = "delete from {0} where DATA_SOURCE = 'AMDB'".format(PROCESS_CONF_TABLE)
    cursor.execute(sql)

    sql = "select PROCESS_ID,DEPLOY_IP,HOST_IP,VIP,PROCESS_DESC,PROCESS_USER,PROCESS_COMMAND,MIN_COUNT,MAX_COUNT,BEGIN_TIME,END_TIME from {0} where VERSION={1}".format(PROCESS_TABLE,version)
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        process_id = row[0]
        deploy_ip = row[1]
        host_ip = row[2]
        vip = row[3]
        process_desc = row[4]
        process_user = row[5]
        process_command = row[6]
        min_count = row[7]
        max_count = row[8]
        begin_time = row[9]
        end_time = row[10]
        process_type = "APP"
        data_source = "AMDB"
        sql = '''insert into {0} (PROCESS_ID,AMDB_VERSION,WRITE_TIME,DEPLOY_IP,HOST_IP,VIP,PROCESS_DESC,PROCESS_USER,PROCESS_COMMAND,MIN_COUNT,MAX_COUNT,PROCESS_TYPE,DATA_SOURCE,BEGIN_TIME,END_TIME) 
               values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''.format(PROCESS_CONF_TABLE)
        para = [process_id, version, write_time, deploy_ip, host_ip, vip, process_desc, process_user,
                            process_command, min_count, max_count,process_type,data_source,begin_time,end_time]
        cursor.execute(sql, para)
        counter += 1

    conn.commit()
    conn.close()
    logger.info("Success insert records:%s", counter)

if __name__ == '__main__':
        main()

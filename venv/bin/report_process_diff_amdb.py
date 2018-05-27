#!/usr/bin/python3
# -*- coding:utf-8 -*-
#Author: Pang Yapeng

import datetime
from sub_common import *

# 设置log
logger = get_logger("report.log")

# 特定参数
PROCESS_CONF_TABLE = "AUTO_PROCESS_CONF"
PROCESS_AMDB_TABLE = "AUTO_AMDB_PROCESS_CONF"

# 设置全局变量
write_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
global counter

def main():
    global counter
    counter = 0

    logger.info("Begin to compare two table:%s,%s",PROCESS_CONF_TABLE,PROCESS_AMDB_TABLE)

    conn = get_conn()
    cursor = conn.cursor()

    sql = "select max(VERSION) from {0}".format(PROCESS_AMDB_TABLE)
    cursor.execute(sql)
    rows = cursor.fetchall()
    cur_version = rows[0][0]

    sql = "select PROCESS_ID,DEPLOY_IP,HOST_IP,VIP,PROCESS_DESC,PROCESS_USER,PROCESS_COMMAND,MIN_COUNT,MAX_COUNT,BEGIN_TIME,END_TIME from {0} where VERSION={1}".format(PROCESS_AMDB_TABLE,cur_version)
    cursor.execute(sql)
    amdb_rows = cursor.fetchall()

    sql = "select PROCESS_ID,DEPLOY_IP,HOST_IP,VIP,PROCESS_DESC,PROCESS_USER,PROCESS_COMMAND,MIN_COUNT,MAX_COUNT,BEGIN_TIME,END_TIME from {0} where PROCESS_TYPE='APP'".format(PROCESS_CONF_TABLE)
    cursor.execute(sql)
    ump_rows = cursor.fetchall()

    amdb_std = set(amdb_rows).difference(set(ump_rows))
    sql = "update {0} set UMP_SYNC_STATUS = 'Y' where VERSION={1}".format(PROCESS_AMDB_TABLE,cur_version)
    cursor.execute(sql)
    if amdb_std:
        for row in amdb_std:
            id = row[0]
            sql = "update {0} set UMP_SYNC_STATUS = 'N' where PROCESS_ID=%s and VERSION={1}".format(PROCESS_AMDB_TABLE,cur_version)
            cursor.execute(sql,id)

    ump_std = set(ump_rows).difference(set(amdb_rows))
    sql = "update {0} set AMDB_SYNC_STATUS = 'Y'".format(PROCESS_CONF_TABLE)
    cursor.execute(sql)
    if ump_std:
        for row in ump_std:
            id = row[0]
            sql = "update {0} set AMDB_SYNC_STATUS = 'N' where PROCESS_ID=%s".format(PROCESS_CONF_TABLE)
            cursor.execute(sql,id)

    conn.commit()
    conn.close()
    logger.info("Record in %s table is not sync to ump:%s", PROCESS_AMDB_TABLE,amdb_std)
    logger.info("Record in %s table is not sync to amdb:%s", PROCESS_CONF_TABLE, ump_std)
    logger.info("Compare complete!")

if __name__ == '__main__':
        main()

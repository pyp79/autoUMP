#!/usr/bin/python3
# -*- coding:utf-8 -*-
#Author: Pang Yapeng

import datetime
from sub_common import *
import uuid

# 设置log
logger = get_logger("process.log")

# 特定参数
PROCESS_CONF_TABLE = "AUTO_PROCESS_CONF"
PROCESS_TABLE = "AUTO_AMDB_PROCESS"
IP_TABLE = "AUTO_AMDB_IP"

# 设置全局变量
write_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
global counter

def main():
    global counter
    counter = 0

    logger.info("Begin to insert to table:%s",PROCESS_CONF_TABLE)

    conn = get_conn()
    cursor = conn.cursor()

    sql = "select max(VERSION) from {0}".format(PROCESS_TABLE)
    cursor.execute(sql)
    rows = cursor.fetchall()
    cur_version = rows[0][0]

    sql = "delete from {0} where VERSION='{1}'".format(PROCESS_CONF_TABLE,cur_version)
    cursor.execute(sql)

    sql = "select distinct(IP_ADDRESS) from {0} where VERSION={1}".format(PROCESS_TABLE,cur_version)
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        ip_address = row[0]
        if is_vip(cur_version,ip_address):
            ip_list = vip_to_hostip(cur_version,ip_address)
            for host_ip in ip_list:
                deploy_ip = host_ip
                vip = ip_address
                sql = "select PROCESS_DESC,PROCESS_USER,PROCESS_COMMAND,MIN_COUNT,MAX_COUNT from {0} where IP_ADDRESS='{1}' and VERSION='{2}'".format(
                    PROCESS_TABLE, ip_address,cur_version)
                cursor.execute(sql)
                rows = cursor.fetchall()
                for row in rows:
                    id = str(uuid.uuid4())
                    process_desc = row[0]
                    process_user = row[1]
                    process_command = row[2]
                    min_count = row[3]
                    max_count = row[4]
                    sql = '''insert into {0} (UUID,VERSION,WRITE_TIME,DEPLOY_IP,HOST_IP,VIP,PROCESS_DESC,PROCESS_USER,PROCESS_COMMAND,MIN_COUNT,MAX_COUNT) 
                          values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''.format(PROCESS_CONF_TABLE)
                    para = [id, cur_version, write_time, deploy_ip, host_ip, vip, process_desc, process_user,
                            process_command, min_count, max_count]
                    cursor.execute(sql, para)
                    counter += 1

        else:
            deploy_ip = ip_address
            host_ip = ip_address
            vip = ip_address

            sql = "select PROCESS_DESC,PROCESS_USER,PROCESS_COMMAND,MIN_COUNT,MAX_COUNT from {0} where IP_ADDRESS='{1}' and VERSION='{2}'".format(PROCESS_TABLE,ip_address,cur_version)
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                id = str(uuid.uuid4())
                process_desc = row[0]
                process_user = row[1]
                process_command = row[2]
                min_count = row[3]
                max_count = row[4]
                sql = '''insert into {0} (UUID,VERSION,WRITE_TIME,DEPLOY_IP,HOST_IP,VIP,PROCESS_DESC,PROCESS_USER,PROCESS_COMMAND,MIN_COUNT,MAX_COUNT) 
                      values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''.format(PROCESS_CONF_TABLE)
                para = [id,cur_version,write_time,deploy_ip,host_ip,vip,process_desc,process_user,process_command,min_count,max_count]
                cursor.execute(sql,para)
                counter += 1
    conn.commit()
    conn.close()
    logger.info("Success insert records:%s", counter)

def is_vip(version,ip_address):
    conn = get_conn()
    cursor = conn.cursor()
    sql = "select IP_TYPE from {0} where IP_ADDRESS='{1}' and VERSION={2}".format(IP_TABLE,ip_address,version)
    cursor.execute(sql)
    rows = cursor.fetchall()
    ip_type = ""
    try:
        ip_type = rows[0][0]
    except:
        logger.warn("Not found ip in table AUTO_AMDB_IP,ip=%s,version=%s" ,ip_address,version)
    if ip_type == "VIP":
        return True
    else:
        return False

def vip_to_hostip(version,ip_address):
    conn = get_conn()
    cursor = conn.cursor()
    sql = "select IP_ADDRESS from {0} where SERVICE_IP='{1}' and VERSION={2}".format(IP_TABLE,ip_address,version)
    cursor.execute(sql)
    rows = cursor.fetchall()
    ip_list = []
    for row in rows:
        ip_list.append(row[0])
    return ip_list


if __name__ == '__main__':
        main()














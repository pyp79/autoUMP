#!/usr/bin/python3
# -*- coding:utf-8 -*-
#Author: Pang Yapeng

import datetime
from sub_common import *

# 设置log
logger = get_logger("process.log")

# 特定参数
PROCESS_CONF_TABLE = "AUTO_AMDB_PROCESS_CONF"
PROCESS_TABLE = "AUTO_AMDB_PROCESS_RAW"
IP_TABLE = "AUTO_AMDB_IP_RAW"

# 设置全局变量
write_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
version = datetime.datetime.now().strftime('%Y%m%d')
global counter
global mapip_to_hostip

def main():
    global counter
    counter = 0

    logger.info("Begin to insert to table:%s",PROCESS_CONF_TABLE)

    conn = get_conn()
    cursor = conn.cursor()

    sql = "delete from {0} where VERSION='{1}'".format(PROCESS_CONF_TABLE,version)
    cursor.execute(sql)

    sql = "select distinct(IP_ADDRESS) from {0} where VERSION={1}".format(PROCESS_TABLE,version)
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        ip_address = row[0]
        if is_vip(version,ip_address):
            ip_list = vip_to_hostip(version,ip_address)
            for host_ip in ip_list:
                deploy_ip = host_ip
                vip = ip_address
                sql = "select PROCESS_DESC,PROCESS_USER,PROCESS_COMMAND,MIN_COUNT,MAX_COUNT,BEGIN_TIME,END_TIME,PROCESS_ID,APP_CODE from {0} where IP_ADDRESS='{1}' and VERSION='{2}'".format(
                    PROCESS_TABLE, ip_address,version)
                cursor.execute(sql)
                rows = cursor.fetchall()
                for row in rows:
                    process_desc = row[0]
                    process_user = row[1]
                    process_command = row[2]
                    min_count = row[3]
                    max_count = row[4]
                    begin_time_raw = row[5]
                    end_time_raw = row[6]
                    process_id = row[7]
                    app_code = row[8]

                    begin_time = int(begin_time_raw.split(':')[0] + begin_time_raw.split(':')[1])
                    end_time = int(end_time_raw.split(':')[0] + end_time_raw.split(':')[1])

                    sql = '''insert into {0} (PROCESS_ID,VERSION,WRITE_TIME,APP_CODE,DEPLOY_IP,HOST_IP,VIP,PROCESS_DESC,PROCESS_USER,PROCESS_COMMAND,MIN_COUNT,MAX_COUNT,BEGIN_TIME,END_TIME) 
                          values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''.format(PROCESS_CONF_TABLE)
                    para = [process_id, version, write_time, app_code, deploy_ip, host_ip, vip, process_desc, process_user,
                            process_command, min_count, max_count,begin_time,end_time]
                    cursor.execute(sql, para)
                    counter += 1

        else:
            mapip_to_hostip()
            try:
                host_ip = mapip_list[ip_address]
                vip = mapip_list[ip_address]
                deploy_ip = ip_address
            except:
                host_ip = ip_address
                vip = ip_address
                deploy_ip = ip_address

            sql = "select PROCESS_DESC,PROCESS_USER,PROCESS_COMMAND,MIN_COUNT,MAX_COUNT,BEGIN_TIME,END_TIME,PROCESS_ID,APP_CODE from {0} where IP_ADDRESS='{1}' and VERSION='{2}'".format(PROCESS_TABLE,ip_address,version)
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                process_desc = row[0]
                process_user = row[1]
                process_command = row[2]
                min_count = row[3]
                max_count = row[4]
                begin_time_raw = row[5]
                end_time_raw = row[6]
                process_id = row[7]
                app_code = row[8]

                begin_time = int(begin_time_raw.split(':')[0] + begin_time_raw.split(':')[1])
                end_time = int(end_time_raw.split(':')[0] + end_time_raw.split(':')[1])

                sql = '''insert into {0} (PROCESS_ID,VERSION,WRITE_TIME,APP_CODE,DEPLOY_IP,HOST_IP,VIP,PROCESS_DESC,PROCESS_USER,PROCESS_COMMAND,MIN_COUNT,MAX_COUNT,BEGIN_TIME,END_TIME) 
                      values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''.format(PROCESS_CONF_TABLE)
                para = [process_id,version,write_time,app_code,deploy_ip,host_ip,vip,process_desc,process_user,process_command,min_count,max_count,begin_time,end_time]
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

def mapip_to_hostip():
    global mapip_list
    mapip_list = {}

    conn = get_conn()
    cursor = conn.cursor()
    sql = "select MAPPINGIP,PHYSICIP from CMDB_OS_RAW where MAPPINGIP != 'None' and USETYPE='生产机'"
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        mapip_list[row[0]] = row[1]
    return mapip_list

if __name__ == '__main__':
        main()














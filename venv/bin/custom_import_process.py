#!/usr/bin/python3
# -*- coding:utf-8 -*-
#Author: Pang Yapeng

import datetime
import csv
import uuid
from sub_common import *

# 设置log
logger = get_logger("import_auto.log")

# 特定参数
conf = get_conf()
PROCESS_INFO = conf.get("SOURCE_FILE", "CUSTOM_PROCESS_INFO")
PROCESS_TABLE = 'AUTO_PROCESS_CONF'

# 设置全局变量
write_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
global counter_add
global counter_delete
global counter_update

def main():
    global counter_add
    global counter_delete
    global counter_update
    counter_add = 0
    counter_delete = 0
    counter_update =0

    logger.info("Begin to insert to table:%s",PROCESS_TABLE)

    conn = get_conn()
    cursor = conn.cursor()

    with open(PROCESS_INFO, encoding="UTF-8") as csvfile:
        data = csv.DictReader(csvfile)
        for row in data:
            action = row['ACTION']
            deploy_ip = row['DEPLOY_IP']
            host_ip = row['HOST_IP']
            vip = row['VIP']
            process_desc = row['PROCESS_DESC']
            process_user = row['PROCESS_USER']
            process_command = row['PROCESS_COMMAND']
            min_count = row['MIN_COUNT']
            max_count = row['MAX_COUNT']
            begin_time = row['BEGIN_TIME']
            end_time = row['END_TIME']
            process_type = row['PROCESS_TYPE']

            key = host_ip + process_user + process_command
            id = str(uuid.uuid3(uuid.NAMESPACE_DNS, key))

            data_source = "CUSTOM"

            if action == 'ADD':
                sql = '''insert into {0} (UUID,WRITE_TIME,DEPLOY_IP,HOST_IP,VIP,PROCESS_DESC,PROCESS_USER,
                    PROCESS_COMMAND,MIN_COUNT,MAX_COUNT,BEGIN_TIME,END_TIME,PROCESS_TYPE,DATA_SOURCE
                    ) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''.format(PROCESS_TABLE)
                para = (id,write_time,deploy_ip,host_ip,vip,process_desc,process_user,process_command,min_count,max_count,begin_time,end_time,process_type,data_source)
                cursor.execute(sql,para)
                counter_add += 1

            elif action == 'DELETE':
                sql = 'delete from {0} where UUID=%s'.format(PROCESS_TABLE)
                cursor.execute(sql,id)
                counter_delete +=1

            elif action == 'UPDATE':
                sql = 'update {0} set WRITE_TIME=%s,MIN_COUNT=%s,MAX_COUNT=%s,BEGIN_TIME=%s,END_TIME=%s,PROCESS_TYPE=%s,DATA_SOURCE=%s where UUID=%s'.format(PROCESS_TABLE)
                print(sql)
                para = (write_time,min_count,max_count,begin_time,end_time,process_type,data_source,id)
                cursor.execute(sql,para)
                counter_update +=1

    conn.commit()
    conn.close()
    logger.info("Insert records:%s,Delete records:%s,Update records:%s", counter_add,counter_delete,counter_update)

if __name__ == '__main__':
        main()


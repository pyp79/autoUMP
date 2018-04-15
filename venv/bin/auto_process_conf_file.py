#!/usr/bin/python3
# -*- coding:utf-8 -*-
#Author: Pang Yapeng

import datetime
from sub_common import *
import os
import filecmp
import shutil


# 设置log
logger = get_logger("process.log")

# 特定参数
conf = get_conf()
CONF_PATH = conf.get("DEST_FILE", "GEN_PROCESS_CONF_PATH")
PROCESS_CONF_TABLE = "AUTO_PROCESS_CONF"

# 设置全局变量
write_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
global counter_process
global counter_ip

def main():
    global counter_process
    global counter_ip
    counter_process = 0
    counter_ip = 0

    logger.info("Begin to generate process config file:%s",CONF_PATH)

    #创建根目录
    mkdir(CONF_PATH)

    #创建配置变更日志文件
    change_log_file = os.path.join(CONF_PATH, "process_change_log.log")


    conn = get_conn()
    cursor = conn.cursor()

    sql = "select max(VERSION) from {0}".format(PROCESS_CONF_TABLE)
    cursor.execute(sql)
    rows = cursor.fetchall()
    cur_version = rows[0][0]

    sql = "select distinct(DEPLOY_IP) from {0} where VERSION={1}".format(PROCESS_CONF_TABLE,cur_version)
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        deploy_ip = row[0]
        config_file_path = os.path.join(CONF_PATH,deploy_ip)
        mkdir(config_file_path)

        config_file = os.path.join(config_file_path,"M03_PROCESS_AMDB.conf")
        config_file_backup = os.path.join(config_file_path,"M03_PROCESS_AMDB.conf.bak")
        config_file_tmp = os.path.join(config_file_path,"M03_PROCESS_AMDB.conf.tmp")

        with open(config_file_tmp,'w',encoding="UTF-8") as file:
            deploy_ip = row[0]
            sql = "select UUID,VIP,PROCESS_DESC,PROCESS_USER,PROCESS_COMMAND,MIN_COUNT,MAX_COUNT from {0} where DEPLOY_IP='{1}' and VERSION={2} ".format(
                PROCESS_CONF_TABLE, deploy_ip,cur_version)
            cursor.execute(sql)
            rs = cursor.fetchall()
            for r in rs:
                uuid = r[0]
                vip = r[1]
                process_desc = r[2]
                process_user = r[3]
                process_command = r[4]
                min_count = r[5]
                max_count = r[6]
                content = "{0}#{1}#{2}#{3}#{4}#{5}#{6}\n".format(uuid,vip,process_desc,process_user,process_command,min_count,max_count)
                file.write(content)

        if os.path.exists(config_file):
            rs = filecmp.cmp(config_file,config_file_tmp)
            if  rs:
                os.unlink(config_file_tmp)
            else:
                shutil.copyfile(config_file,config_file_backup)
                shutil.move(config_file_tmp,config_file)

                change_log_content = deploy_ip + "\n"
                if os.path.exists(change_log_file):
                    with open(change_log_file, 'a', encoding="UTF-8") as change_log:
                        change_log.write(change_log_content)
                else:
                    with open(change_log_file, 'w', encoding="UTF-8") as change_log:
                        change_log.write(change_log_content)

        else:
            shutil.move(config_file_tmp,config_file)
            change_log_content = deploy_ip + "\n"
            if os.path.exists(change_log_file):
                with open(change_log_file, 'a', encoding="UTF-8") as change_log:
                    change_log.write(change_log_content)
            else:
                with open(change_log_file, 'w', encoding="UTF-8") as change_log:
                    change_log.write(change_log_content)
        pass
    conn.close()


def mkdir(path):
    # 去除首位空格
    path = path.strip()
    # 去除尾部 \ 符号
    path = path.rstrip("\\")

    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    isExists = os.path.exists(path)

    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        # 创建目录操作函数
        os.makedirs(path)
        return True
    else:
        # 如果目录存在则不创建，并提示目录已存在
        return False

if __name__ == '__main__':
        main()



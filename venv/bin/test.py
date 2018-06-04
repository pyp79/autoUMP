#!/usr/bin/python3
# -*- coding:utf-8 -*-
#Author: Pang Yapeng

import re
import sys
import logging
import pymysql
import os
import configparser

import hashlib

# 待加密信息
str = 'EBMP'+'PBGateway'+'10.1.96.3'+'ebmp'

# 创建md5对象
hl = hashlib.md5()

# Tips
# 此处必须声明encode
# 若写法为hl.update(str)  报错为： Unicode-objects must be encoded before hashing
#hl.update(str)
hl.update(str.encode(encoding='utf-8'))

print('MD5加密前为 ：' + str)
print('MD5加密后为 ：' + hl.hexdigest())


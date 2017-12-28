# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author yangyuhong@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年7月1日 上午1:47:11
#
################################################################################
"""
Created on 2017年11月25日

@author: yangyh
"""
import random
from libs import util
from libs import log

DATA_TYPE_MAINFEED = "MAINFEED"
DATA_TYPE_AUTHOR = "AUTHOR"
DATA_TYPE_KEYWORD = "KEYWORD"
DATA_TYPE_TAG_KEYWORD = "TAG_KEYWORD"
DATA_TYPE_TAG_NAME = "TAG_NAME"
DATA_TYPE_VIDEO = "VIDEO"
DATA_PROVIDER = "kuaishou"

MONGO_TABLE_TOPIC = "m_topic"
MONGO_TABLE_VIDEO = "m_video"
MONGO_TABLE_AUTHOR = "m_author"


DATA_VERSION = 1


device_info_list = util.load_file_asarray("./data/kuaishou_device_list")
device_info_list = [x.split(',') for x in device_info_list]
device_info_list = [{"ud": x[1].strip(), 
                     "token": x[3].strip(), 
                     "token_client_salt": x[2].strip(), 
                     "did": x[4].strip(),
                     "mobile": x[0].strip()} for _, x in enumerate(device_info_list)]

log.debug("Load kuaishou devices {}".format(len(device_info_list)))


def randomDevice():
    return random.choice(device_info_list)
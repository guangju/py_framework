# -*- coding=utf-8 -*-
import random
from libs import util
from libs import log

DATA_TYPE_AUTHOR = "AUTHOR"
DATA_TYPE_TOPIC = "TOPIC"
DATA_TYPE_VIDEO = "VIDEO"
DATA_TYPE_MAINFEED = "MAINFEED"
DATA_TYPE_MAINFEEDUP = "MAINFEEDUP"

DATA_PROVIDER = "huoshan"

MONGO_TABLE_TOPIC = "m_topic"
MONGO_TABLE_VIDEO = "m_video"
MONGO_TABLE_AUTHOR = "m_author"

DATA_VERSION = 1


device_id_list = util.load_file_asarray("./data/huoshan_device_list")

log.debug("Load huoshan devices {}".format(len(device_id_list)))

def get_iid():
    iid = "17719964990"
    return str(random.randint(1, 10000) + int(iid))


def get_device_id():
    #device_id = "39891196278"
    #return str(random.randint(1, 10000) + int(device_id))
    length = len(device_id_list)
    r = random.randint(0, length - 1)
    return device_id_list[r]


def get_uuid():
    uuid = "86141130317358549"
    return str(random.randint(1, 10000) + int(uuid))

def get_openudid():
    openudid = "9d0ac3c7b8300138"
    return openudid[-5] + str(random.randint(10000, 99999))

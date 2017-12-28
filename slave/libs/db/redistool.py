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
Created on 2017年11月28日

@author: yangyh
"""
import redis
from libs import conftool
from libs import log

REDIS_HOST = conftool.cf.get("redis", "host")
REDIS_PORT = conftool.cf.get("redis", "port")

redisServ = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

KEY = conftool.cur_env + ":queue2cspub" 

KEY_DEQUEUED = conftool.cur_env + ":dequeued" 

def getMaxInQueue():
    r = redisServ.zrevrange(KEY, 0, 0, withscores=True)
    return r


def enQueue(itemKey, priority, queueName=KEY):
    return redisServ.zadd(queueName, itemKey, priority)


def recordOutput(itemKey, priority):
    redisServ.zadd(KEY_DEQUEUED, itemKey, priority)
    return

def deQueue(queueName=KEY, withScore=False):
    while True:
        itemKey = redisServ.zrevrange(queueName, 0, 0, withscores=withScore)
        if len(itemKey) == 0:
            if withScore:
                return False, False
            return False
        if withScore:
            rkey = itemKey[0][0]
        else:
            rkey = itemKey[0]
        if redisServ.zrem(queueName, rkey) > 0:
            return itemKey[0]

def pushtToList(key, value):
    log.debug("{}: {}".format(key, value))
    redisServ.lpush(key, value)

def popFromList(key):
    return redisServ.lpop(key)

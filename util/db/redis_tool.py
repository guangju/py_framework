

import redis
from log import log


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

def confSet(field, key, value):
    log.debug("set to redis:{} {} {}".format(field, key, value))
    redisServ.hset(fileld, key, value)

def confGet(field, key):
    log.debug("get from redis:{} {}".format(field, key))

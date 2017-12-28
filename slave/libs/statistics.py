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
Created on 2017年11月23日

@author: yangyh
"""
from __future__ import division, print_function, unicode_literals
from libs.db import redistool, queue
from libs import util, conftool

class Statistics(object):
    """
    classdocs
    """
    PROVIDERS = ["kuaishou", "douyin", "huoshan"]
    def __init__(self, params=None):  # @UnusedVariable
        """
        Constructor
        """
        self.insertedObjects = {}
        self.pipes = {}
        self.counter = {}

    def incrProviderSend2cspub(self, provider):
        self.incrRedis(conftool.cur_env + ":sended2cspub:" + provider)
        return

    def getProviderSend2cspub(self):
        return {k: self.getRedisValue(conftool.cur_env + ":sended2cspub:" + k) for k in self.PROVIDERS}
    
    def incrRedis(self, key, incrVal=1, expire=-1):    
        if type(incrVal) is float:
            resp = redistool.redisServ.incrbyfloat(key, incrVal)
        else:
            resp = redistool.redisServ.incr(key, incrVal)
        if expire > 0 and resp == incrVal:
            redistool.redisServ.expire(key, expire)
        return
    
    def incrCspubResult(self, result):
        redistool.redisServ.incr("cspubresult:{}".format(result), 1)
        
    def getCspubResult(self):
        keys = redistool.redisServ.keys("cspubresult*")
        if len(keys) == 0:
            return {}
        return {key: redistool.redisServ.get(key) for key in keys}
    
    def getCspubSpiderDelay(self):
        keys = redistool.redisServ.keys("cspub:delay:" + util.current_hour() + ":*")
        if len(keys) == 0:
            return {}
        resp = {}
        for k in keys:
            try:
                avgDelay = float(redistool.redisServ.get(k)) / float(redistool.redisServ.get(k.replace(":delay:", ":callback:")))
                avgDelay = str(avgDelay * 1000) + "(s)"
                newKey = k.split(":")[2:4]
                resp[":".join(newKey)] = avgDelay
            except Exception as e:
                resp[k] = str(e)
        return resp
           
    def getCspubSpiderSent(self):
        keys = redistool.redisServ.keys("cspub:sent:{}:*".format(util.current_hour()))
        if len(keys) == 0:
            return {}
        resp = {}
        for k in keys:
            try:
                sent = redistool.redisServ.get(k)
                back = redistool.redisServ.get(k.replace(":sent:", ":callback:"))
                avgDelay = "sent:{}, back:{}".format(sent, back)
                newKey = k.split(":")[2:4]
                resp[":".join(newKey)] = avgDelay
            except Exception as e:
                resp[k] = str(e)
        return resp 
    
    def getSaverError(self):
        keys = redistool.redisServ.keys("saver:status-v2:{}:*".format(util.current_hour()))
        if len(keys) == 0:
            return {}
        return {key: redistool.redisServ.get(key) for key in keys}
    
    def getRedisValue(self, key):
        return redistool.redisServ.get(key)

    def setRedisValue(self, key, value):
        return redistool.redisServ.set(key, value)

    def incr(self, key):
        if key in self.counter:
            self.counter[key] += 1
        else:
            self.counter[key] = 1
    
    def addInserted(self, objectId, objectType="default"):
        if objectType not in self.insertedObjects:
            self.insertedObjects[objectType] = {}
        self.insertedObjects[objectType][objectId] = self.insertedObjects[objectType].get(objectId, 0) + 1
    
    def getPipeByName(self, pipeName):
        return self.pipes.get(pipeName)
            
    def addPipe(self, pipe):
        self.pipes[pipe.name] = pipe
    
    def getQueueInfo(self):
        q = queue.JobPriorityQueue()
        qBackup = queue.JobBackupQueue()
        return {
            "numRemain": q.getQueueSize(),
            "numOut": qBackup.getQueueSize()
            }
    
    def incrSenderCallback(self):
        self.incrRedis("totalCallback")
        self.incrRedis(util.current_date() + ":totalCallback")
        self.incrRedis(util.current_hour() + ":totalCallback")
        
    def getSenderCallback(self):
        resp = {}
        resp["total"] = self.getRedisValue("totalCallback")
        resp["today"] = self.getRedisValue(util.current_date() + ":totalCallback")
        resp["yesterday"] = self.getRedisValue(util.current_date(-1) + ":totalCallback")
        resp["hour"] = self.getRedisValue(util.current_hour() + ":totalCallback")
        resp["hour-1"] = self.getRedisValue(util.current_hour(-1) + ":totalCallback")
        return resp
    
    def getInsertedInfo(self):
        return {
            "total": {k: sum(v.values()) for k, v in self.insertedObjects.iteritems()},
            "dedup": {k: len(v) for k, v in self.insertedObjects.iteritems()},
            "pipes": {x.name: {"status": x.isRunning(), 
                               "queue": x.getQueueSize(),
                               "numTotalAdded": x.getAddedSize(),
                               "numNeedFetch": x.numFetchAll,
                               "numFetchErr": x.numFetchErr,
                               "numTemplateErr": x.numTemplateErr,
                               'numToWorker': x.numToWorker,
                               'numPending': x.getNumPending(),
                               "lastSaveTime": x.lastSaveTime} for x in self.pipes.values()
                      },
            "counter": self.counter,
            "totalSenderCallback": self.getRedisValue("totalCallback"),
            "senderCallback": self.getSenderCallback(),
            "providerSend2cspub": self.getProviderSend2cspub(),
            "cspubResult": self.getCspubResult(),
            "cspubMetaDelay": self.getCspubSpiderDelay(),
            "capubSentBack": self.getCspubSpiderSent(),
            "saverError": self.getSaverError()
            }
        
        
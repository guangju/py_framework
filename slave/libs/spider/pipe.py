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
Created on 2017年11月22日

@author: yangyh
"""
from __future__ import division, print_function, unicode_literals
import threading
import traceback
import time
import random
import cPickle
import os
from libs.db import redistool
from libs import log
from multiprocessing.pool import ThreadPool

class Message(object):
    
    def __init__(self, msgType, msgData, priority=100, extra={}):  # @UnusedVariable
        self.msgType = msgType
        self.msgData = msgData
        self.priority = priority
        self.retry = 0
        self.extra = {}
        
    def __str__(self):
        return "msgType={},msgData={},extra={}".format(self.msgType, self.msgData, self.extra)
    
    def getExtra(self, key):
        return self.extra.get(key)
    
    def setExtra(self, key, val):
        self.extra[key] = val
    
    def getKey(self, key, default=None):
        return self.extra.get(key, default)
    
    setKey = setExtra
    addKey = setExtra
        

class Pipe(threading.Thread):
    """
    classdocs
    """
    saveDir = "queue"

    def __init__(self, urlGenerator, crawler, saver, name="default",
                stat=None, priority=10, numThreads=5):
        """
        Constructor
        """
        threading.Thread.__init__(self)
        self.urlGenerator = urlGenerator
        self.urlGenerator.setPipe(self)
        self.crawler = crawler
        self.crawler.setStatistics(stat)
        
        self.saver = saver
        self.saver.setPipe(self)
        self.downstream = set()
        self.name = name
        self.stat = stat
        self.running = False
        self.numFetchErr = 0
        self.numFetchAll = 0
        self.numTemplateErr = 0
        self.numToWorker = 0
        stat.addPipe(self)
        self.pool = ThreadPool(processes=numThreads + 1)
        self.pending = set()
        self.lastSaveTime = 0
        self.lastGetPriTime = 0
        self.load()
        self.priorityName = self.name + "_priority"
        self.priority = self.setPriority(priority)
        self.pool.apply_async(self.save, ())

        log.notice("pipe {} start".format(name))
        return

    def setPriority(self, priority):
        pri = self.stat.getRedisValue(self.priorityName)
        if pri == None:
            pri = priority
            self.stat.setRedisValue(self.priorityName, priority)

        return int(pri)

    def getPriority(self):
        pri = self.priority
        if self.lastGetPriTime + 300 < time.time():
            pri = int(self.stat.getRedisValue(self.priorityName))

        return pri

    def save(self):
        while True:
            time.sleep(300 + random.randint(1, 200))
            self._save()
    
    def load(self):
        fn = self.saveDir + "/" + self.name
        num = 0
        try:
            if os.path.exists(fn):
                with open(fn, "rb") as f:
                    tmpSet = cPickle.load(f)
                    num = len(tmpSet)
                    for urlPack in tmpSet:
                        self.urlGenerator.add(urlPack)
                    log.notice("load {} queue, num={}".format(self.name, num))
        except Exception as e:
            log.fatal("load_queue_error_{}".format(self.name), e)
        return num
    
    def _save(self):
        if not os.path.exists(self.saveDir):
            os.makedirs(self.saveDir)
        target = self.saveDir + "/" + self.name
        tmpFile = target + ".tmp"
        num = len(self.pending)
        if num == 0:
            return 0
        with open(tmpFile, "wb") as f:
            cPickle.dump(self.pending, f, protocol=-1)
            log.notice("save {} queue, num={}".format(self.name, num))
            self.lastSaveTime = time.time()
            os.rename(tmpFile, target)
        return num
    
    def getNumPending(self):
        return len(self.pending)
    
    def getQueueSize(self):
        return self.urlGenerator.getQueueSize()
    
    def getAddedSize(self):
        return self.urlGenerator.getAddedSize()
    
    def addMessageObject(self, msg):
        return self.urlGenerator.onReceiveMsg(msg)
    
    def addMessage(self, msgType, msgData, priority=0):
        msg = Message(msgType, msgData)
        msg.setExtra('priority', priority)
        return self.urlGenerator.onReceiveMsg(msg)
    
    def isRunning(self):
        return self.running
    
    def _receive(self, msg):
        log.debug("{} receive a message!".format(self.name))
        self.urlGenerator.onReceiveMsg(msg)
        return
    
    def publish(self, msg):
        for n in self.downstream:
            n._receive(msg)
        return
    
    def _addDownstream(self, pipe):
        self.downstream.add(pipe)
        return
    
    def listenOutputOfPipe(self, anotherPipe):
        #if anotherPipe == self:
        #    return False
        anotherPipe._addDownstream(self)
        return True
        
    def incTemplateError(self):
        self.numTemplateErr += 1
    
    def _real_worker(self, urlPacker):
        try:
            self.numFetchAll += 1
            log.notice("{},{}".format(threading.current_thread().name, urlPacker))
            resp = self.crawler.fetch(urlPacker)
            if resp is False:
                log.fatal("{} fetch_error:{}".format(self.name, urlPacker))
                self.numFetchErr += 1
                return
            if type(resp) == bool:
                #cspub model
                return
            self.saver.start(resp, urlPacker)
            self.urlGenerator.done(urlPacker)
        except Exception as e:
            self.running = False
            log.fatal(e)
            traceback.print_exc()
        finally:
            self.pending.discard(urlPacker)
            
    
    def run(self):
        log.debug("pipe {} priority: {}".format(self.name, self.priority))
        try:
            self.running = True
            while not self.urlGenerator.end() and self.running:
                urlPacker = self.urlGenerator.next()
                self.pool.apply_async(self._real_worker, (urlPacker, ))
                self.pending.add(urlPacker)
                self.numToWorker += 1
        except Exception as e:
            log.fatal("{} end with error {}".format(self.name, e))
        self.running = False
        return


    def pushToRetryList(self, value):
        redistool.pushtToList("RetryMsgList_" + self.name, value)

    def popFromRetryList(self):
        return redistool.popFromList("RetryMsgList_" + self.name)

    def pushToTrashList(self, value):
        redistool.pushtToList("TrashMsgList_" + self.name, value)

    def popFromTrashList(self):
        return redistool.popFromList("TrashMsgList_" + self.name)

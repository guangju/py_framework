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
from libs import log
from libs import conftool
import Queue
import cPickle
import base64


RETRY = int(conftool.cf.get("retry", "retry"))


class UrlPack(object):

    def __init__(self, priority, url):
        #优先级越小, 越快被抓取
        self.priority = priority
        self.url = url
        self.form = ''
        self.extra = {}
        return

    def addKey(self, key, val):
        self.extra[key] = val

    def getKey(self, key, default=None):
        return self.extra.get(key, default)

    def setForm(self, form):
        self.form = form

    def getExtra(self):
        return self.extra

    def fillMsg(self, msg, pipe):
        self.addKey('msgType', msg.msgType)
        self.addKey('msgData', msg.msgData)
        self.addKey('retry', msg.getKey('retry', 0))
        self.addKey('pipe', pipe.name)
        self.addKey("priority", msg.getKey("priority", 0))
        self.addKey('pipeLine', msg.getKey('pipeLine', 0))
        self.msg = msg

    def __cmp__(self, other):
        return cmp(self.priority, other.priority)

    def __str__(self):
        return "[url={}, form={}, extra={}, priority={}]".format(self.url,
                                                  self.form,
                                                  self.extra,
                                                  self.priority
                                                )


class UrlProvider(object):
    """
    classdocs
    """
    PERSIST_DIR = "queue"

    def __init__(self, params=None):
        """
        Constructor
        """
        log.notice(params)
        self.queue = Queue.PriorityQueue()
        self.queueElements = set()
        self.numAdded = 0
        self.pipe = None
        return

    def load(self):
        with open(self.PERSIST_DIR + "/" + self.pipe.name, "rb") as f:
            queueElements = cPickle.load(f)
            for e in queueElements:
                self.add(e)
        return len(queueElements)

    def save(self):
        with open(self.PERSIST_DIR + "/" + self.pipe.name, "wb") as f:
            cPickle.dump(self.queueElements, f, protocol=0)
        return len(self.queueElements)

    def setPipe(self, pipe):
        self.pipe = pipe

    def getQueueSize(self):
        return self.queue.qsize()

    def getAddedSize(self):
        return self.numAdded

    def add(self, urlPack):
        self.numAdded += 1
        self.queueElements.add(urlPack)
        self.queue.put(urlPack)

    def addUrl(self, url, priority=0):
        self.numAdded += 1
        pack = UrlPack(priority, url)
        self.queueElements.add(UrlPack)
        self.queue.put(pack)

    def onReceiveMsg(self, msg):
        log.debug("Receive a message: {}".format(msg))
        return False

    def done(self, urlPack):
        self.queueElements.discard(urlPack)

    def next(self):
        tmp = self.queue.get(True)
        #log.debug("UrlProvider next: {}".format(tmp))
        return tmp

    def end(self):
        return False

    def check(self):
        return False

    def getPipePriority(self):
        return self.pipe.getPriority()

    def queueGet(self):
        while True:
            try:
                pack = self.queue.get(True, 5)
                return pack
            except:
                if RETRY == 1 or self.pipe.name == "DouyinTopicVideosPipeCspub":
                    msg = self.pipe.popFromTrashList()
                    if not msg:
                        continue
                    else:
                        log.debug("get one from retry list")
                        msg = cPickle.loads(base64.b64decode(msg))
                        self.pipe.addMessageObject(msg)

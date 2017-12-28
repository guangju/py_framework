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
version
"""
from __future__ import division, print_function, unicode_literals
import basehandler
import time
from libs.const import const
from libs import log, conftool
from libs.db import queue
from libs import statistics
from libs.adapter import adaptertool
from libs.db import spider_ucptool, dbtools, mongo

CALLBACK_HOST = conftool.cf.get('sender_callback', 'host')
CALLBACK_PORT = conftool.cf.get('sender_callback', 'port')

class JobHandler(basehandler.BaseRmbHandler):
    """
    classdocs
    """

    def work(self):
        """
            main worker
        """
        log.notice("in JobHandler handler")
        cmd = self.getParamAsString('cmd')
        if cmd == "get":
            #从队列提取一条item
            try:
                q = queue.JobPriorityQueue()
                itemKey, priority = q.deQueue(True)
                if itemKey is False:
                    self.response_data = {"notice": "queue empty"}
                    return
                self.response_data = {"_key_": itemKey}
                queueBack = queue.JobBackupQueue()
                queueBack.enQueue(itemKey, time.time())
                _, provider, thirdId, _ = dbtools.get_key_info(itemKey)
                isCrawled = spider_ucptool.isVideoCrawled("{}_{}".format(provider, thirdId))
                db = mongo.DB()
                if isCrawled:
                    insertVal = {}
                    insertVal["_crawl_"] = const.CRAWL_STATUS_OK
                    insertVal["_utime_"] = int(time.time())
                    db.updateByKey(const.getTable(itemKey), itemKey, insertVal)
                    self.response_data = {"_key_": itemKey, "_crawl_": const.CRAWL_STATUS_OK}
                    return
                data = db.getOne(const.getTable(itemKey), itemKey, '_key_')   
                uid = adaptertool.getUid(data)
                authorKey = "AUTHOR-{}-{}-1".format(provider, uid)
                data['_authorInfo_'] = db.getOne(const.getTable(const.DATA_TYPE_AUTHOR), authorKey, '_key_')
                data['_callback_'] = "http://" + conftool.randomChoice(CALLBACK_HOST, CALLBACK_PORT) + "/job?cmd=callback&_key_=" + itemKey
                data['_priority_'] = priority
                if len(data.get('_topic3rdId_', '')) > 0:
                    try:
                        topicKey = "TOPIC-{}-{}-1".format(provider, data['_topic3rdId_'])
                        topicInfo = db.getOne(const.getTable('TOPIC'), topicKey, '_key_')
                        data['microVideoTopic'] = adaptertool.transform(topicKey, topicInfo)['microVideoTopic']
                    except Exception as e:
                        log.warning("error_get_microVideoTopic", e)
                    
                self.response_data = data        
                log.notice("pop one not crawled:{}".format(itemKey))
            except Exception as e:
                log.fatal("error_get_job_fromqueue={}, _key_={}".format(e, itemKey))
                self.response_data = {"_key_": itemKey, "error": str(e)}
            return
        if cmd == "add":
            itemKey = self.checkParamAsString('_key_')
            priority = self.getParamAsInt('priority', 10000)
            q = queue.JobPriorityQueue()
            resp = q.enQueue(itemKey, priority)
            self.response_data = resp
            return
        if cmd == "callback":
            itemKey = self.checkParamAsString('_key_')
            log.notice("got a callback:{}".format(itemKey))
            db = mongo.DB()
            stat = statistics.Statistics()
            value = {}
            value["_crawl_"] = 1
            value["_utime_"] = int(time.time())
            if self.getParamAsString('from') == 'mimod':
                value['_cspubResult_'] = self.getParamAsString('result', '')
                stat.incrCspubResult(value['_cspubResult_'])
            resp = db.updateByKey(const.getTable(itemKey), itemKey, value)
            self.response_data = {"_key_": itemKey, "_crawl_": 1, 'resp': resp}
            stat.incrSenderCallback()
            return
        raise ValueError("invalid cmd: ".format(cmd))
        
    def initialize(self, **kwarg):
        """
            init
        """
        pass

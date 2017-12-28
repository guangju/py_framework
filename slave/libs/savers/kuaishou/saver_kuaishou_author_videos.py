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
from libs.savers.kuaishou import saver_kuaishou_base
from libs.db import dbtools
from jsonpath_rw import parse
from libs.const import const_kuaishou
from libs.spider.pipe import Message
import time

class KuaiShouVideoListSaver(saver_kuaishou_base.KuaishouSaverBase):
    
    def __init__(self, db=None):
        self.db = db
        self.xpath_actions = []
        self.register_action("$", self.handler)
        return
    
    def handler(self, root, data, urlPack):  # @UnusedVariable
        feeds = data["feeds"]
        pcursor = data["pcursor"]
        for info in feeds:
            info[self.pipe.name] = int(time.time())
            obj = dbtools.MongoObject()
            obj.setMeta(const_kuaishou.DATA_TYPE_VIDEO, const_kuaishou.DATA_PROVIDER, info["photo_id"])
            obj.setData(info)
            obj.setUserId(info['user_id'])
            obj.save(const_kuaishou.MONGO_TABLE_VIDEO)
            log.debug("KuaiShouAuthorVideoListSaver Inserting obj {}".format(obj.getLastObjectId()))
            self.addStatObject(obj.getLastObjectId(), const_kuaishou.DATA_TYPE_VIDEO)
            authorId = info['user_id']
        log.debug("KuaiShouAuthorVideoListSaver feed length: {}, pcursor: {}".format(len(feeds), pcursor))
        if len(feeds) > 0:
            msg = Message(const_kuaishou.DATA_TYPE_AUTHOR, authorId)
            msg.setExtra("pcursor", pcursor)
            self.publish(msg)
        return
    
    def preCheck(self, video, urlPack):
        exprs = [parse("$.feeds"), parse("$.pcursor")]
        return self.checkTemplate(video, exprs, urlPack)
        
    
    
    
    
    
    
        
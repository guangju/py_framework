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
from libs.const import const_douyin
from libs.savers.douyin import saver_douyin_base
from libs.db import mongo
from libs.db import dbtools
from libs import log
from jsonpath_rw import parse

class DouyinTopicDetailSaver(saver_douyin_base.DouyinSaverBase):
    
    type_douyin = 3
    
    DATA_TYPE_TOPIC = "TOPIC"
    DATA_TYPE_VIDEO = "VIDEO"
    DATA_TYPE_TOPIC_DETAIL = "TOPIC_DETAIL"
    DATA_VERSION = 1
    
    def __init__(self, db=None):
        if db is None:
            db = mongo.DB()
        self.db = db
        self.xpath_actions = []
        self.register_action("$.ch_info", self.handleChallangeDetail)
        return
    
    def handleChallangeDetail(self, root, data, urlPack):  # @UnusedVariable
        log.debug(data)
        data["cha_name"] = data["cha_name"].strip()
        data["desc"] = data["desc"].strip()
        obj = dbtools.MongoObject()
        obj.setMeta(const_douyin.DATA_TYPE_TOPIC, const_douyin.DATA_PROVIDER, data["cid"], version=self.DATA_VERSION)
        obj.setData(data)
        obj.save(const_douyin.MONGO_TABLE_TOPIC)
        self.addStatObject(obj.getLastObjectId(), const_douyin.DATA_TYPE_TOPIC_DETAIL)
        log.debug("DouyinTopicDetailSaver Insert obj {}".format(obj.getLastObjectId()))
        return
    
    
    def preCheck(self, video, urlPack):
        exprs = [parse("$.ch_info.author.uid")
                 ]
        return self.checkTemplate(video, exprs, urlPack)
        
    
    
    
    
    
    
        
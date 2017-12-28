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
from libs.savers.douyin import saver_douyin_base
from libs.db import dbtools
from jsonpath_rw import parse
from libs.const import const_douyin

class DouyinVideoDetailSaver(saver_douyin_base.DouyinSaverBase):
    
    def __init__(self, db=None):
        self.db = db
        self.xpath_actions = []
        self.register_action("$.aweme_detail", self.handleAwemeDetail)
        return
    
    def handleAwemeDetail(self, root, data, urlPack):  # @UnusedVariable
        if type(data) == dict:
            data = [data]
        for aweme in data:
            obj = dbtools.MongoObject()
            obj.setMeta(const_douyin.DATA_TYPE_VIDEO, const_douyin.DATA_PROVIDER, aweme["aweme_id"], version=const_douyin.DATA_VERSION)
            obj.setData(aweme)
            obj.save(const_douyin.MONGO_TABLE_VIDEO)
            log.debug("DouyinVideoDetailSaver Inserting obj {}".format(obj.getLastObjectId()))
            self.addStatObject(obj.getLastObjectId(), const_douyin.DATA_TYPE_VIDEO)
        return
    
    def preCheck(self, video, urlPack):
        exprs = [parse("$.aweme_detail.aweme_id")]
        return self.checkTemplate(video, exprs, urlPack)
        
    
    
    
    
    
    
        
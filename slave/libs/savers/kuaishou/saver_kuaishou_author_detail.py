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
from libs.db import dbtools
from jsonpath_rw import parse
from libs.const import const_kuaishou
from libs.savers.kuaishou import saver_kuaishou_base

class KuaiShouAuthorDetailSaver(saver_kuaishou_base.KuaishouSaverBase):
    
    def __init__(self, db=None):
        self.db = db
        self.xpath_actions = []
        self.register_action("$.userProfile", self.handler)
        return
    
    def handler(self, root, data, urlPack):  # @UnusedVariable
        if type(data) == dict:
            data = [data]
        for info in data:
            obj = dbtools.MongoObject()
            obj.setMeta(const_kuaishou.DATA_TYPE_AUTHOR, const_kuaishou.DATA_PROVIDER, info["profile"]["user_id"], version=const_kuaishou.DATA_VERSION)
            obj.setData(info)
            obj.save(const_kuaishou.MONGO_TABLE_AUTHOR)
            log.debug("KuaiShouAuthorDetailSaver Inserting obj {}".format(obj.getLastObjectId()))
            self.addStatObject(obj.getLastObjectId(), const_kuaishou.DATA_TYPE_AUTHOR)
        return
    
    def preCheck(self, video, urlPack):
        exprs = [parse("$.userProfile.profile.user_id")]
        return self.checkTemplate(video, exprs, urlPack)
        
    
    
    
    
    
    
        
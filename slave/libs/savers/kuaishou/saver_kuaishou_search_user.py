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
import time
from libs import log
from libs.spider.pipe import Message
from libs.savers.kuaishou import saver_kuaishou_base
from libs.db import dbtools, mongo
from jsonpath_rw import parse
from libs.const import const_kuaishou, const

class KuaiShouSearchUserSaver(saver_kuaishou_base.KuaishouSaverBase):
    
    def __init__(self, db=None):
        if db is None:
            db = mongo.DB()
        self.db = db
        self.xpath_actions = []
        self.register_action("$.users", self.handler)
        return
    
    def handler(self, root, users, urlPack):  # @UnusedVariable
        log.debug("return users len:[{}]".format(len(users)))
        for user in users:
            key = dbtools.gen_object_key('AUTHOR', 'kuaishou', user['user_id'])
            if not self.db.isObjectUpdatedRecently(const.getTable('AUTHOR'), key, 365 * 86400):
                log.debug("search result, pcursor={}, user_id={}".format(urlPack.getKey('pcursor'), user['user_id']))
                msg = Message(const.DATA_TYPE_AUTHOR, user['user_id'])
                self.publish(msg)
                obj = dbtools.MongoObject()
                obj.setMeta(const.DATA_TYPE_AUTHOR, const_kuaishou.DATA_PROVIDER, user['user_id'])
                obj.setData(user)
                obj.save()
                log.debug("KuaiShouSearchUserSaver Inserting obj {}".format(obj.getLastObjectId()))
                self.addStatObject(obj.getLastObjectId(), const_kuaishou.DATA_TYPE_VIDEO)
                
                #authorDetail
                p = self.pipe.stat.getPipeByName('KuaiShouAuthorDetailPipeCspub')
                msg = Message('AUTHOR', user['user_id'])
                p.addMessageObject(msg)
                
                if int(urlPack.getKey('pcursor')) <= 10:
                    p = self.pipe.stat.getPipeByName('KuaiShouAuthorVideosPipeCspub')
                    msg = Message('AUTHOR', user['user_id'])
                    p.addMessageObject(msg)
                
        if type(users) is list and len(users) > 0:
            time.sleep(10)
            msg = Message(const_kuaishou.DATA_TYPE_KEYWORD, urlPack.extra['keyword'])
            msg.addKey('pcursor', int(urlPack.extra['pcursor']) + 1)
            self.publish(msg)
            log.debug("publish to next page: {}".format(self.pipe.name))
        return
    
    def preCheck(self, root, urlPack):
        exprs = [parse("$.users")]
        return self.checkTemplate(root, exprs, urlPack)
        
    
    
    
    
    
    
        
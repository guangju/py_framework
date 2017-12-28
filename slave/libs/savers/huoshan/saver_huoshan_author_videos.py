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

from libs.spider.pipe import Message
from libs import log
from libs import saver
from libs.db import dbtools
from jsonpath_rw import parse
from libs.const import const_huoshan, const

class HuoshanAudioVideoListSaver(saver.Saver):
    
    def __init__(self, db=None):
        self.db = db
        self.xpath_actions = []
        self.register_action("$.data", self.handler)
        return
    
    def handler(self, root, dataList, urlPack):  # @UnusedVariable
        for entity in dataList:
            data = entity['data']
            authorId = data['author']['id']
            del data["author"]
            obj = dbtools.MongoObject()
            obj.setMeta(const.DATA_TYPE_VIDEO, const_huoshan.DATA_PROVIDER, data["id"])
            obj.setData(data)
            obj.setUserId(authorId)
            obj.save()
            log.debug("HuoshanAuthorVideoListSaver Inserting obj {}".format(obj.getLastObjectId()))
            self.addStatObject(obj.getLastObjectId(), const.DATA_TYPE_VIDEO)
        if root['extra']['has_more']:
            msg = Message(const.DATA_TYPE_AUTHOR, authorId)
            msg.setExtra('max_time', root['extra']['max_time'])
            self.publish(msg)
        return
    
    def preCheck(self, video, urlPack):
        exprs = [parse("$.data[*].data.author.id"), parse("$.data[*].data.id")]
        return self.checkTemplate(video, exprs, urlPack)
        
    
    
    
    
    
    
        
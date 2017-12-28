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
from libs.db import dbtools
from libs.db import mongo
from libs import log
from jsonpath_rw import parse
from libs.spider.pipe import Message

class DouyinTopicSaver(saver_douyin_base.DouyinSaverBase):
    
    
    def __init__(self, db=None):
        if db is None:
            db = mongo.DB()
        self.db = db
        self.xpath_actions = []
        #self.register_action("$.category_list[*].aweme_list[*].video.play_addr.url_list", self.replace_https)
        self.register_action("$.category_list[*].challenge_info", self.handleChallengeInfo)
        self.register_action("$.category_list[*].aweme_list", self.handleAwemeList)
        #self.register_action(None,self.add_extend_fileds)
        return
    
    def handleAwemeList(self, root, data, urlPack):  # @UnusedVariable
        for music in data:
            obj = dbtools.MongoObject()
            obj.setMeta(const_douyin.DATA_TYPE_VIDEO, const_douyin.DATA_PROVIDER, music["aweme_id"], version=self.DATA_VERSION)
            obj.setData(music)
            obj.save()
            log.debug("DouyinTopicSaver Insert obj {}".format(obj.getLastObjectId()))
            self.addStatObject(obj.getLastObjectId(), const_douyin.DATA_TYPE_VIDEO)
            self.publish(Message(const_douyin.DATA_TYPE_VIDEO, music["aweme_id"]))
        return
    
    def handleChallengeInfo(self, root, data, urlPack):  # @UnusedVariable
        log.debug("handleChallengeInfo", data)
        obj = dbtools.MongoObject(self.db)
        obj.setMeta(const_douyin.DATA_TYPE_TOPIC, const_douyin.DATA_PROVIDER, data["cid"])
        if not self.db.isObjectUpdatedRecently(const_douyin.MONGO_TABLE_TOPIC, obj.key):
            self.publish(Message(const_douyin.DATA_TYPE_TOPIC, data["cid"]))
        return

    def preCheck(self, video, urlPack):
        exprs = [parse("$.category_list[*].challenge_info.cha_name"),
                 parse("$.category_list[*].challenge_info.cid"),
                 parse("$.category_list[*].challenge_info.user_count"),
                 parse("$.category_list[*].aweme_list"),
                 parse("$.category_list[*].aweme_list[*].statistics.aweme_id"),
                 parse("$.category_list[*].aweme_list[*].statistics.play_count"),
                 parse("$.category_list[*].aweme_list[*].video.origin_cover.url_list")
                 ]
        return self.checkTemplate(video, exprs, urlPack)
        
        
    
    
    
    
    
    
        
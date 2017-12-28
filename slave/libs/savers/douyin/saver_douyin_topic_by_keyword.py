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
from libs.savers.douyin import saver_douyin_base
from libs import log
from libs.db import dbtools
from libs.db import mongo
from jsonpath_rw import parse
from libs.spider.pipe import Message
from libs.const import const_douyin


class DouyinTopicByKeywordSaver(saver_douyin_base.DouyinSaverBase):

    def __init__(self, db=None):
        if db is None:
            db = mongo.DB()
        self.db = db
        self.xpath_actions = []
        self.register_action("$", self.handleChallengeInfo)
        return

    def handleChallengeInfo(self, root, dataDict, urlPack):  # @UnusedVariable
        for data in dataDict['challenge_list']:
            data = data['challenge_info']
            if data['user_count'] < 10:
                continue
            obj = dbtools.MongoObject(self.db)
            obj.setMeta(const_douyin.DATA_TYPE_TOPIC, const_douyin.DATA_PROVIDER, data["cid"])
            obj.setData(data)
            #最近一天更新过
            if not self.db.isObjectUpdatedRecently(const_douyin.MONGO_TABLE_TOPIC, obj.key, 86400):
                #self.publish(Message(const_douyin.DATA_TYPE_TOPIC, data["cid"]))
                obj.save(const_douyin.MONGO_TABLE_TOPIC)
                log.debug("DouyinTopicByKeywordSaver Inserting obj _key_={}, user_count={}".format(obj.key, data['user_count']))
                msg = Message(const_douyin.DATA_TYPE_TOPIC, data["cid"])
                self.publish(msg)
                self.addStatObject(obj.getLastObjectId(), "TOPIC")

        if dataDict['has_more'] > 0:
            msg = Message(const_douyin.DATA_TYPE_TOPIC_KEYWORD, urlPack.getKey('keyword'))
            msg.setExtra('keyword', urlPack.getKey('keyword'))
            msg.setExtra('cursor', urlPack.getKey('cursor', 0) + 20)
            self.publish(msg)
        return

    def preCheck(self, video, urlPack):
        exprs = [parse("$.challenge_list"), parse("$.has_more")]
        return self.checkTemplate(video, exprs, urlPack)

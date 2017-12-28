# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author xuke13@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年11月30日
#
################################################################################
"""
Created on 2017年11月30日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals

from libs import log
from libs.savers.douyin import saver_douyin_base
from libs.db import dbtools
from jsonpath_rw import parse
from libs.const import const_douyin
from libs.spider.pipe import Message

class DouyinAuthorVideoSaver(saver_douyin_base.DouyinSaverBase):

    def __init__(self, db=None):
        self.db = db
        self.xpath_actions = []
        self.register_action("$", self.handleUserDetail)
        return

    def handleUserDetail(self, root, data, urlPack):  # @UnusedVariable
        cursor = data["max_cursor"]
        aweme_list = data["aweme_list"]
        for aweme in aweme_list:
            vid = aweme["aweme_id"]
            uid = aweme["author_user_id"]
            obj = dbtools.MongoObject()
            obj.setMeta(const_douyin.DATA_TYPE_VIDEO, const_douyin.DATA_PROVIDER, vid, version=const_douyin.DATA_VERSION)
            obj.setData(aweme)
            obj.setUserId(uid)
            obj.save(const_douyin.MONGO_TABLE_VIDEO)
            log.debug("DouyinAuthorVideoSaver Inserting obj {}".format(obj.getLastObjectId()))
            self.addStatObject(obj.getLastObjectId(), const_douyin.DATA_TYPE_VIDEO)

        if data["has_more"] == 1:
            msg = Message(const_douyin.DATA_TYPE_AUTHOR, uid)
            msg.setExtra("cursor", cursor)
            self.publish(msg)
        else:
            log.debug("DouyinAuthorVideoSaver: no more!")

        return

    def preCheck(self, video, urlPack):
        exprs = [parse("$.aweme_list"),parse("$.max_cursor")]
        return self.checkTemplate(video, exprs, urlPack)

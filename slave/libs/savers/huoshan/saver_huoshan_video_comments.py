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
Created on 2017年12月7日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals

from libs.spider.pipe import Message
from libs import log
from libs import saver
from libs.db import dbtools, mongo
from jsonpath_rw import parse
from libs.const import const_huoshan, const

class HuoshanVideoCommentsSaver(saver.Saver):

    def __init__(self, db=None):
        if db is None:
            db = mongo.DB()
        self.db = db
        self.xpath_actions = []
        self.register_action("$", self.handler)
        return

    def handler(self, root, data, urlPack):  # @UnusedVariable
        comments = data["data"]["comments"]
        vid = urlPack.getKey("vid")
        offset = urlPack.getKey("offset")

        for comment in comments:
            uid = comment["user"]["id"]
            log.debug("HuoshanVideoComments get one uid: {}".format(uid))
            authorKey = dbtools.gen_object_key('AUTHOR', const_huoshan.DATA_PROVIDER, uid)
            if not self.db.isItemUpdatedRecently(authorKey, 3 * 86400):
                msg = Message(const_huoshan.DATA_TYPE_AUTHOR, uid)
                self.publish(msg)
            else:
                log.debug("huoshan user_id:{} has already updated".format(uid))


        if data['extra']['has_more']:
            msg = Message(const.DATA_TYPE_VIDEO, vid)
            msg.setExtra('offset', offset + 1)
            self.publish(msg)
        return

    def preCheck(self, video, urlPack):
        exprs = [parse("$.data.comments")]
        return self.checkTemplate(video, exprs, urlPack)

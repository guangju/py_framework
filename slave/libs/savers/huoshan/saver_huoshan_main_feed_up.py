# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author xuke13@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年12月12日
#
################################################################################
"""
Created on 2017年12月12日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals

from libs import log
from libs import saver
from libs.db import dbtools, mongo
from jsonpath_rw import parse
from libs.const import const_huoshan
from libs.spider.pipe import Message

class HuoShanMainFeedUpSaver(saver.Saver):

    def __init__(self, db=None):
        if db is None:
            db = self.db = mongo.DB()
        self.db = db
        self.xpath_actions = []
        self.register_action("$", self.handler)
        return

    def handler(self, root, data, urlPack):  # @UnusedVariable
        log.debug("huoshan main_feed_up saver handler, len={}".format(len(data["data"])))
        for info in data["data"]:
            vid = str(info['data']['id'])
            uid = str(info['data']['author']['id'])
            obj = dbtools.MongoObject()
            #视频直接存下来
            obj = dbtools.MongoObject()
            obj.setMeta("VIDEO", const_huoshan.DATA_PROVIDER, vid)
            obj.setUserId(uid)
            obj.setData(info)
            if not self.db.isItemUpdatedRecently(obj.key):
                obj.save()
                log.debug("Inserting obj from HuoshanMainFeedUp video: {}".format(obj.getLastObjectId()))
            else:
                log.debug("HuoshanMainFeedUp video: {} already inserted".format(obj.getLastObjectId()))

            #如果作者三天以上未更新, 则publish uid
            authorKey = dbtools.gen_object_key('AUTHOR', const_huoshan.DATA_PROVIDER, uid)
            if not self.db.isItemUpdatedRecently(authorKey, 3 * 86400):
                self.addStatObject(authorKey, const_huoshan.DATA_TYPE_AUTHOR)
                msg = Message(const_huoshan.DATA_TYPE_AUTHOR, uid)
                self.pipe.publish(msg)
            else:
                log.debug("author updated recently")

        return


    def preCheck(self, video, urlPack):
        exprs = [parse("$.data"), parse("$.data[*].data.id"), parse("$.data[*].data.author.id")]
        return self.checkTemplate(video, exprs, urlPack)

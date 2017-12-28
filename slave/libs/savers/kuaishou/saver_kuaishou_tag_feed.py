# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author xuke13@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年12月5日
#
################################################################################
"""
Created on 2017年12月5日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals

from libs import log
from libs.spider.pipe import Message
from libs.savers.kuaishou import saver_kuaishou_base
from libs.db import dbtools, mongo
from jsonpath_rw import parse
from libs.const import const_kuaishou
import time

class KuaiShouTagFeedSaver(saver_kuaishou_base.KuaishouSaverBase):

    def __init__(self, db=None):
        if db is None:
            db = mongo.DB()
        self.db = db
        self.xpath_actions = []
        self.register_action("$", self.handler)
        return

    def handler(self, root, data, urlPack):  # @UnusedVariable
        feeds = data["feeds"]
        pcursor = data["pcursor"]
        tag = urlPack.getKey("tag")
        log.debug("KuaiShouVideoListSaver tag:{}, feed length: {}, pcursor: {}".format(tag, len(feeds), pcursor))
        for info in feeds:
            info[self.pipe.name] = int(time.time())
            obj = dbtools.MongoObject()
            obj.setMeta(const_kuaishou.DATA_TYPE_VIDEO, const_kuaishou.DATA_PROVIDER, info["photo_id"], version=const_kuaishou.DATA_VERSION)
            obj.setData(info)
            obj.setUserId(info['user_id'])
            obj.setTopicId(urlPack.getKey("topic_id"))
            obj.save()
            log.debug("KuaiShouTagFeedSaver Inserting obj {}, tag={}".format(obj.getLastObjectId(), tag))
            self.addStatObject(obj.getLastObjectId(), const_kuaishou.DATA_TYPE_VIDEO)
            authorId = info['user_id']

            author_obj = dbtools.MongoObject(db=self.db)
            author_obj.setMeta(const_kuaishou.DATA_TYPE_AUTHOR, const_kuaishou.DATA_PROVIDER, authorId)
            if not self.db.isItemUpdatedRecently(author_obj.key, 3 * 86400):
                msg = Message(const_kuaishou.DATA_TYPE_AUTHOR, authorId)
                self.publish(msg)
            else:
                log.debug("skip user_id:{}".format(authorId))
        
        if pcursor != "no_more":
            msg = Message(const_kuaishou.DATA_TYPE_TAG_NAME, tag)
            msg.setExtra("topic_id", urlPack.getKey("topic_id"))
            msg.setExtra("pcursor", pcursor)
            self.publish(msg)
            time.sleep(60)

        return

    def preCheck(self, root, urlPack):
        exprs = [parse("$.feeds"), parse("$.pcursor"),
        parse("$.feeds[*].main_mv_urls")]
        return self.checkTemplate(root, exprs, urlPack)

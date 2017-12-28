# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author gonglixing@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年12月5日
#
################################################################################
"""
Created on 2017年12月5日

@author: gonglixing
"""
from __future__ import division, print_function, unicode_literals

from libs import log
from libs.db import dbtools, mongo
from jsonpath_rw import parse
from libs.const import const_douyin
from libs.spider.pipe import Message
from libs.savers.douyin import saver_douyin_base

class DouYinMainFeedSaver(saver_douyin_base.DouyinSaverBase):

    def __init__(self, db=None):
        if db is None:
            db = mongo.DB()
        self.db = db
        self.xpath_actions = []
        self.register_action("$", self.handler)
        return

    def handler(self, root, data, urlPack):  # @UnusedVariable
        log.debug("douyin main feed saver handler, len={}".format(len(data["aweme_list"])))
        for info in data["aweme_list"]:
            vid = str(info['aweme_id'])
            uid = str(info['author']['uid'])
            
            #视频直接存下来
            obj = dbtools.MongoObject()
            obj.setMeta("VIDEO", const_douyin.DATA_PROVIDER, vid)
            obj.setUserId(uid)
            obj.setData(info)
            if not self.db.isItemUpdatedRecently(obj.key):
                obj.save()
                log.debug("Inserting obj from DouyinMainFeed: {}".format(obj.getLastObjectId()))

            #如果作者三天以上未更新, 则publish uid
            authorKey = dbtools.gen_object_key('AUTHOR', const_douyin.DATA_PROVIDER, uid)
            if not self.db.isItemUpdatedRecently(authorKey, 3 * 86400):
                objAuthor = dbtools.MongoObject()
                objAuthor.setMeta("AUTHOR", const_douyin.DATA_PROVIDER, uid)
                objAuthor.save()
                self.addStatObject(authorKey, const_douyin.DATA_TYPE_AUTHOR)
                msg = Message(const_douyin.DATA_TYPE_AUTHOR, uid)
                self.pipe.publish(msg)
            else:
                log.notice("douyin author updated recently")
        return


    def preCheck(self, video, urlPack):
        exprs = [parse("$.aweme_list"), parse("$.aweme_list[*].aweme_id"), parse("$.aweme_list[*].author.uid")]
        return self.checkTemplate(video, exprs, urlPack)

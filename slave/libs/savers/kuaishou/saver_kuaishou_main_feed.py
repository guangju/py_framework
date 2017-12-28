# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author xuke13@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年11月29日
#
################################################################################
"""
Created on 2017年11月29日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals

from libs import log
from libs.db import dbtools, mongo
from jsonpath_rw import parse
from libs.const import const_kuaishou
from libs.spider.pipe import Message
from libs.savers.kuaishou import saver_kuaishou_base

class KuaiShouMainFeedSaver(saver_kuaishou_base.KuaishouSaverBase):

    def __init__(self, db=None):
        if db is None:
            db = mongo.DB()
        self.db = db
        self.xpath_actions = []
        self.register_action("$", self.handler)
        return

    def handler(self, root, data, urlPack):  # @UnusedVariable
        log.debug("kuaishou main feed saver handler, len={}".format(len(data["feeds"])))
        for info in data["feeds"]:
            vid = str(info['photo_id'])
            uid = str(info['user_id'])
            
            #视频直接存下来
            obj = dbtools.MongoObject()
            obj.setMeta("VIDEO", const_kuaishou.DATA_PROVIDER, vid)
            obj.setUserId(uid)
            obj.setData(info)
            if not self.db.isItemUpdatedRecently(obj.key):
                obj.save()
                log.debug("Inserting obj from KuaishouMainFeed: {}".format(obj.getLastObjectId()))
            
            #如果作者三天以上未更新, 则publish uid
            authorKey = dbtools.gen_object_key('AUTHOR', const_kuaishou.DATA_PROVIDER, uid)
            if not self.db.isItemUpdatedRecently(authorKey, 3 * 86400):
                objAuthor = dbtools.MongoObject()
                objAuthor.setMeta("AUTHOR", const_kuaishou.DATA_PROVIDER, uid)
                objAuthor.save()
                self.addStatObject(authorKey, const_kuaishou.DATA_TYPE_AUTHOR)
                msg = Message(const_kuaishou.DATA_TYPE_AUTHOR, uid)
                self.pipe.publish(msg)
            else:
                log.notice("kuaishou author updated recently")

        return


    def preCheck(self, video, urlPack):
        exprs = [parse("$.feeds"), parse("$.pcursor")]
        return self.checkTemplate(video, exprs, urlPack)

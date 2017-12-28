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
version
"""
from __future__ import division, print_function, unicode_literals
import basehandler
from libs import log, conftool
from libs.db import mongo, dbtools
from libs.const import const
from libs.adapter import adaptertool


CALLBACK_HOST = conftool.cf.get('sender_callback', 'host')
CALLBACK_PORT = conftool.cf.get('sender_callback', 'port')

class ItemInfoHandler(basehandler.BaseRmbHandler):
    """
    classdocs
    """

    def work(self):
        """
            main worker
        """
        log.notice("in ItemInfoHandler handler")
        key = self.checkParamAsString('key')
        db = mongo.DB()
        table = const.getTable(key)
        itemType, provider, thirdId, version = dbtools.get_key_info(key)
        resp = db.getOne(table, dbtools.get_object_id_by_key(key))
        if resp is None:
            self.response_data = resp
            return
        adapter = __import__('libs.adapter.adapter_' + str(key.split("-")[1]), fromlist=["libs.adapter"])
        if itemType == "VIDEO":
            uid = adaptertool.getUid(resp)
            authorKey = "AUTHOR-{}-{}-1".format(provider, uid)
            authorInfo = db.getOne(const.getTable(const.DATA_TYPE_AUTHOR), authorKey, '_key_')
            if authorInfo is None:
                log.fatal("no author info for key:{}".format(key))
                raise ValueError("no author meta")
                return
            resp['_authorInfo_'] = authorInfo
            resp['_callback_'] = "http://" + conftool.randomChoice(CALLBACK_HOST, CALLBACK_PORT) + "/job?cmd=callback&_key_=" + key
            resp = adaptertool.transform(key, resp)
        elif itemType == "AUTHOR":
            resp = adapter.transformAuthorDetail(resp)
        else:
            raise ValueError("Invalid itemType")
        
        self.response_data = resp
        log.notice("get iteminfo: {},{},{},{}".format(itemType, provider, thirdId, version))
        
    def initialize(self, **kwarg):
        """
            init
        """
        self.statistics = kwarg['statistics']
        pass

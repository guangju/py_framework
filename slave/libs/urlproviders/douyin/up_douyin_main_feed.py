# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author gonglixing@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年12月4日
#
################################################################################
"""
Created on 2017年12月4日

@author: gonglixing
"""
from __future__ import division, print_function, unicode_literals
from libs import log
from libs.db import mongo
from libs import urlprovider
from libs.spider import signature
from libs.const import const_douyin


class DouyinMainFeedProvider(urlprovider.UrlProvider):
    """
    classdocs
    """
    url = 'http://api.amemv.com/aweme/v1/feed/?type=0&max_cursor=0&min_cursor=0&count=6&aweme_retry_count=0&iid={iid}&device_id={device_id}&ac=wifi&channel=vivo&aid=1128&app_name=aweme&version_code=156&version_name=1.5.6&device_platform=android&ssmix=a&device_type=vivo+Y67A&device_brand=vivo&os_api=23&os_version=6.0&uuid={uuid}&openudid={openudid}&manifest_version_code=156&resolution=720*1280&dpi=320&update_version_code=1561&app_type=normal'

    def __init__(self):
        urlprovider.UrlProvider.__init__(self)
        self.db = mongo.DB()
        return

    def onReceiveMsg(self, msg):
        """
        :param msg:
        """
        log.debug("DouyinMainFeedProvider receive {}".format(msg))
        if msg.msgType == const_douyin.DATA_TYPE_MAINFEED:
            target = self.url.format(iid=const_douyin.get_iid(),
                                device_id=const_douyin.get_device_id(),
                                openudid=const_douyin.get_openudid(),
                                uuid=const_douyin.get_uuid())
            urlPack = urlprovider.UrlPack(priority=0, url=target)
            urlPack.fillMsg(msg, self.pipe)
            self.add(urlPack)
            return True
        return False

    def next(self):
        urlPack = self.queueGet()
        url = signature.sign(urlPack.url, signature.aweme)
        urlPack.url = url['url']
        urlPack.priority = self.getPipePriority()
        log.debug("DouyinMainFeedProvider next: {}".format(urlPack))
        return urlPack

    def end(self):
        return False

    def check(self):
        return False

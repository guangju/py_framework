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
from libs.const import const_huoshan


class HuoshanMainFeedProvider(urlprovider.UrlProvider):
    """
    classdocs
    """
    url = 'http://hotsoon.snssdk.com/hotsoon/feed/?type=video&min_time=0&count=20&req_from=feed_refresh&live_sdk_version=272&iid={iid}&device_id={device_id}&ac=wifi&channel=goapk&aid=1112&app_name=live_stream&version_code=272&version_name=2.7.2&device_platform=android&ssmix=a&device_type=H60-L02&device_brand=Huawei&os_api=19&os_version=4.4.2&uuid={uuid}&openudid={openudid}&manifest_version_code=272&resolution=1080*1776&dpi=480&update_version_code=2722'

    def __init__(self):
        urlprovider.UrlProvider.__init__(self)
        self.db = mongo.DB()
        return

    def onReceiveMsg(self, msg):
        """
        :param msg:
        """
        log.debug("HuoshanMainFeedProvider receive {}".format(msg))
        if msg.msgType == const_huoshan.DATA_TYPE_MAINFEED:
            target = self.url.format(
                iid=const_huoshan.get_iid(),
                device_id=const_huoshan.get_device_id(),
                openudid=const_huoshan.get_openudid(),
                uuid=const_huoshan.get_uuid()
                )
            urlPack = urlprovider.UrlPack(priority=0, url=target)
            urlPack.fillMsg(msg, self.pipe)
            self.add(urlPack)
            return True
        return False

    def next(self):
        urlPack = self.queueGet()
        url = signature.sign(urlPack.url, signature.hotsoon)
        urlPack.url = url['url']
        urlPack.priority = self.getPipePriority()
        log.debug("HuoshanMainFeedProvider next: {}".format(urlPack))
        return urlPack

    def end(self):
        return False

    def check(self):
        return False

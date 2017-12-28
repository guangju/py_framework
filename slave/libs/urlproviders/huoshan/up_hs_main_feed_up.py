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
from libs.db import mongo
from libs import urlprovider
from libs.spider import signature
from libs.const import const_huoshan
import time


class HuoshanMainFeedUpProvider(urlprovider.UrlProvider):
    """
    classdocs
    """

    url = "http://hotsoon.snssdk.com/hotsoon/feed/?type=video&max_time={time}&count=20&req_from=feed_loadmore&live_sdk_version=272&iid={iid}&device_id={device_id}&ac=wifi&channel=local&aid=1112&app_name=live_stream&version_code=272&version_name=2.7.2&device_platform=android&ssmix=a&device_type=vivo+Y67A&device_brand=vivo&os_api=23&os_version=6.0&uuid={uuid}&openudid={openudid}&manifest_version_code=272&resolution=720*1280&dpi=320&update_version_code=2722"


    def __init__(self):
        urlprovider.UrlProvider.__init__(self)
        self.db = mongo.DB()
        return

    def onReceiveMsg(self, msg):
        """
        :param msg:
        """
        log.debug("HuoshanMainFeedUpProvider receive {}".format(msg))
        if msg.msgType == const_huoshan.DATA_TYPE_MAINFEEDUP:
            target = self.url.format(
                time=time.time(),
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
        log.debug("HuoshanMainFeedUpProvider next: {}".format(urlPack))
        return urlPack

    def end(self):
        return False

    def check(self):
        return False

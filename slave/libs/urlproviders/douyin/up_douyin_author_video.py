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
from libs import urlprovider
from libs.spider import signature
from libs.const import const_douyin


class DouyinAuthorVideoProvider(urlprovider.UrlProvider):
    """
    classdocs
    """

    url = "http://api.amemv.com/aweme/v1/aweme/post/?user_id={user_id}&max_cursor={cursor}&count=10&aweme_retry_count=0&iid={iid}&device_id={device_id}&ac=wifi&channel=vivo&aid=1128&app_name=aweme&version_code=156&version_name=1.5.6&device_platform=android&ssmix=a&device_type=vivo+Y67A&device_brand=vivo&os_api=23&os_version=6.0&uuid={uuid}&openudid={openudid}&manifest_version_code=156&resolution=720*1280&dpi=320&update_version_code=1561&app_type=normal"

    def __init__(self):
        urlprovider.UrlProvider.__init__(self)
        return

    def onReceiveMsg(self, msg):
        """
        :param msg:
        """
        log.debug("DouyinAuthorVideoProvider receive {}".format(msg))
        if msg.msgType == const_douyin.DATA_TYPE_AUTHOR:
            cursor = 0
            if msg.getExtra("cursor") is None:
                cursor = 0
            else:
                cursor = msg.getExtra("cursor")

            target = self.url.format(
                user_id=msg.msgData,
                cursor = cursor,
                iid=const_douyin.get_iid(),
                device_id=const_douyin.get_device_id(),
                openudid=const_douyin.get_openudid(),
                uuid=const_douyin.get_uuid()
                )
            urlPack = urlprovider.UrlPack(priority=20, url=target)
            urlPack.fillMsg(msg, self.pipe)
            self.add(urlPack)
            return True
        return False

    def next(self):
        urlPack = self.queueGet()
        url = signature.sign(urlPack.url, signature.aweme)
        urlPack.url = url['url']
        urlPack.priority = self.getPipePriority()
        log.debug("DouyinAuthorVideoProvider next: {}".format(urlPack))
        return urlPack

    def end(self):
        return False

    def check(self):
        return False

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
Created on 2017年11月22日

@author: yangyuhong
"""
from __future__ import division, print_function, unicode_literals
from libs import log
from libs import urlprovider
from libs.spider import signature
from libs.const import const_huoshan, const

class HuoshanAuthorVideosUrlProvider(urlprovider.UrlProvider):
    """
        classdocs
    """
    url_first = "http://hotsoon.snssdk.com/hotsoon/user/{user_id}/items/?count=30&req_from=enter_auto&live_sdk_version=272&iid={iid}&device_id={device_id}&ac=wifi&channel=goapk&aid=1112&app_name=live_stream&version_code=272&version_name=2.7.2&device_platform=android&ssmix=a&device_type=H60-L02&device_brand=Huawei&os_api=19&os_version=4.4.2&uuid={uuid}&openudid={openudid}&manifest_version_code=272&resolution=1080*1776&dpi=480&update_version_code=2722"
    url_next = "http://hotsoon.snssdk.com/hotsoon/user/{user_id}/items/?max_time={max_time}&count=30&req_from=enter_auto&live_sdk_version=272&iid={iid}&device_id={device_id}&ac=wifi&channel=goapk&aid=1112&app_name=live_stream&version_code=272&version_name=2.7.2&device_platform=android&ssmix=a&device_type=H60-L02&device_brand=Huawei&os_api=19&os_version=4.4.2&uuid={uuid}&openudid={openudid}&manifest_version_code=272&resolution=1080*1776&dpi=480&update_version_code=2722"


    def __init__(self):
        urlprovider.UrlProvider.__init__(self)
        return

    def onReceiveMsg(self, msg):  # @UnusedVariable
        """
        :param msg:
        """
        if msg.msgType == const.DATA_TYPE_AUTHOR:
            user_id = msg.msgData
            if msg.getExtra("max_time") is None:
                target = self.url_first.format(user_id=user_id,
                                        iid=const_huoshan.get_iid(),
                                        device_id=const_huoshan.get_device_id(),
                                        uuid=const_huoshan.get_uuid(),
                                        openudid=const_huoshan.get_openudid()
                                        )
            else:
                target = self.url_next.format(max_time=msg.getExtra("max_time"),
                                        user_id=user_id,
                                        iid=const_huoshan.get_iid(),
                                        device_id=const_huoshan.get_device_id(),
                                        uuid=const_huoshan.get_uuid(),
                                        openudid=const_huoshan.get_openudid()
                                        )
            urlPack = urlprovider.UrlPack(priority=20, url=target)
            urlPack.fillMsg(msg, self.pipe)
            self.add(urlPack)
            return True
        return False

    def next(self):
        urlPack = self.queueGet()
        url = signature.sign(urlPack.url, signature.hotsoon)
        urlPack.url = url['url']
        urlPack.priority = self.getPipePriority()
        log.debug("HuoshanAuthorVideosUrlProvider next: {}".format(urlPack))
        return urlPack

    def end(self):
        return False

    def check(self):
        return False

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

@author: yangyh
"""
from __future__ import division, print_function, unicode_literals
from libs import log
from libs import urlprovider
from libs.spider import signature
from libs.const import const_douyin


class DouyinVideoDetailProvider(urlprovider.UrlProvider): 
    """
    classdocs
    """
    
    url = "http://aweme.snssdk.com/aweme/v1/aweme/detail/?aweme_id={aweme_id}&aweme_retry_count=0&iid={iid}&device_id={device_id}&ac=wifi&channel=vivo&aid=1128&app_name=aweme&version_code=156&version_name=1.5.6&device_platform=android&ssmix=a&device_type=vivo+Y67A&device_brand=vivo&os_api=23&os_version=6.0&uuid={uuid}&openudid={openudid}&manifest_version_code=156&resolution=720*1280&dpi=320&update_version_code=1561&app_type=normal"
    
    def __init__(self):
        urlprovider.UrlProvider.__init__(self)
        return
    
    def onReceiveMsg(self, msg):
        """
        :param msg:
        """
        log.debug("DouyinVideoDetailProvider receive ".format(msg))
        if msg.msgType == const_douyin.DATA_TYPE_VIDEO:
            target = self.url.format(
                aweme_id=msg.msgData,
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
        log.debug("DouyinVideoDetailProvider next: {}".format(urlPack))
        return urlPack
    
    def end(self):
        return False
    
    def check(self):
        return False
        
    
    

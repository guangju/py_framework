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


class DouyinTopicUrlProvider(urlprovider.UrlProvider): 
    """
    classdocs
    """
    
    url = "http://api.amemv.com/aweme/v1/category/list/?cursor={cursor}&count=5&retry_type=no_retry&iid={iid}&device_id={device_id}&ac=wifi&channel=update&aid=1128&app_name=aweme&version_code=162&version_name=1.6.2&device_platform=android&ssmix=a&device_type=MIX&device_brand=Xiaomi&os_api=24&os_version=7.0&uuid={uuid}&openudid={openudid}&manifest_version_code=162&resolution=1920*1080&dpi=440&update_version_code=1622&app_type=normal"
    
    def __init__(self, iterateTime=0):  # @UnusedVariable
        urlprovider.UrlProvider.__init__(self)
        '''
        for cursor in range(0, iterateTime, 1):
            u = self.url.format(cursor=cursor, iid=const_douyin.get_iid(),
                                device_id=const_douyin.get_device_id(),
                                openudid=const_douyin.get_openudid(),
                                uuid=const_douyin.get_uuid())
            self.add(urlprovider.UrlPack(0, u.format(cursor)))
        '''
        return
    
    def onReceiveMsg(self, msg):  # @UnusedVariable
        """

        :param msg:
        """
        if msg.msgType == const_douyin.DATA_TYPE_TOPIC:
            target = self.url.format(cursor=msg.msgData, iid=const_douyin.get_iid(),
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
        log.debug("DouyinTopicUrlProvider next: {}".format(urlPack))
        return urlPack
    
    def end(self):
        return False
    
    def check(self):
        return False
        
    

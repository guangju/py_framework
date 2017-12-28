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
from libs.spider import signature
from libs import urlprovider
from libs.const import const_douyin

class DouyinVideosByChallenge(urlprovider.UrlProvider):

    @staticmethod
    def concat(cid, offset):
        u = 'http://api.amemv.com/aweme/v1/challenge/aweme/?ch_id={ch_id}&cursor={cursor}&count=20&type=5&retry_type=no_retry&iid={iid}&device_id={device_id}&ac=wifi&channel=update&aid=1128&app_name=aweme&version_code=162&version_name=1.6.2&device_platform=android&ssmix=a&device_type=MIX&device_brand=Xiaomi&os_api=24&os_version=7.0&uuid={uuid}&openudid={openudid}&manifest_version_code=162&resolution=1920*1080&dpi=440&update_version_code=1622&app_type=normal'
        u = u.format(ch_id=cid,
                 cursor=offset,
                 uuid=const_douyin.get_uuid(),
                 iid=const_douyin.get_iid(),
                 device_id=const_douyin.get_device_id(),
                 openudid=const_douyin.get_openudid()
                 )
        return u

    def onReceiveMsg(self, msg):
        #仅处理话题
        if msg.msgType == const_douyin.DATA_TYPE_TOPIC:
            for offset in range(0, 5):
                #self.addUrl(self.concat(msg.msgData, offset))
                target = self.concat(msg.msgData, offset)
                urlPack = urlprovider.UrlPack(priority=20, url=target)
                urlPack.fillMsg(msg, self.pipe)
                self.add(urlPack)
            return True
        return False

    def next(self):
        log.debug("size:{}".format(self.queue.qsize()))
        urlPack = self.queueGet()
        url = signature.sign(urlPack.url, signature.aweme)
        urlPack.url = url['url']
        urlPack.priority = self.getPipePriority()
        log.debug("DouyinVideoUrlByChallenge next: {}".format(urlPack))
        return urlPack

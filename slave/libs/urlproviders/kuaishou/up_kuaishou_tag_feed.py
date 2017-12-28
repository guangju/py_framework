# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author xuke13@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年12月5日
#
################################################################################
"""
Created on 2017年12月5日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals
from libs import log
from libs import urlprovider
from libs.spider import signature
from libs.const import const_kuaishou


class KuaiShouTagFeedProvider(urlprovider.UrlProvider):
    """
    classdocs
    """

    url = ("http://api.gifshow.com/rest/n/feed/tag?appver=5.4.4.302&"
           "did={did}&c=a&ver=5.4&ud={ud}&lon=115.3676372022031&"
           "lat=41.04144830295876&sys=ios11.1.2&mod=iPhone7%2C2&net=%E4%B8%AD%E5%9B%BD%E8%81%94%E9%80%9A_5")
           
    form = ("client_key=3c2cd3f3&count=20&country_code=cn&"
            "language=zh-Hans-CN%3Bq%3D1&token={token}&tag={tag}")

    form_second = "client_key=3c2cd3f3&count=20&country_code=cn&language=zh-Hans-CN%3Bq%3D1&token={token}&tag={tag}&pcursor={pcursor}"

    #ext = {
    #    'token_client_salt': "92647ba6ba3c8a362bd2c3440d0b6233"
    #}

    def __init__(self, iterateTime=0):  # @UnusedVariable
        urlprovider.UrlProvider.__init__(self)
        return

    def onReceiveMsg(self, msg):  # @UnusedVariable
        """
        :param msg:
        """
        if msg.msgType == const_kuaishou.DATA_TYPE_TAG_NAME:
            device = const_kuaishou.randomDevice()
            target = self.url.format(did=device['did'], ud=device['ud'])
            urlPack = urlprovider.UrlPack(priority=20, url=target)
            if msg.getExtra('pcursor') == None:
                urlPack.setForm(self.form.format(tag=msg.msgData, token=device['token']))
            else:
                urlPack.setForm(self.form_second.format(
                    tag=msg.msgData,
                    pcursor=msg.getExtra('pcursor'),
                    token=device['token']))

            urlPack.fillMsg(msg, self.pipe)
            urlPack.addKey('tag', msg.msgData)
            urlPack.addKey('topic_id', msg.getExtra('topic_id'))
            urlPack.addKey('ext', {"token_client_salt": device['token_client_salt']})
            self.add(urlPack)
            return True
        return False

    def next(self):
        urlPack = self.queueGet()
        info = signature.sign(urlPack.url, signature.gifshow, urlPack.form, urlPack.getKey('ext'))
        urlPack.url = info['url']
        urlPack.form = info['form']
        urlPack.priority = self.getPipePriority()
        log.debug("KuaiShouTagFeedProvider next: {}".format(urlPack))
        return urlPack

    def end(self):
        return False

    def check(self):
        return False

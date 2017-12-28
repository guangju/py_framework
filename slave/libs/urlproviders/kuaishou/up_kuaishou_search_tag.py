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
import urllib


class KuaiShouSearchTagProvider(urlprovider.UrlProvider):
    """
    classdocs
    """

    url = ("http://api.gifshow.com/rest/n/tag/search?appver=5.4.4.302&"
           "did={did}&c=a&ver=5.4&ud={ud}&lon=116.2676372022031&lat=40.04144830295876&"
           "sys=ios11.1.2&mod=iPhone7%2C2&net=%E4%B8%AD%E5%9B%BD%E8%81%94%E9%80%9A_5")
    form = ("client_key=3c2cd3f3&count=20&country_code=cn&"
            "language=zh-Hans-CN%3Bq%3D1&token={token}&keyword={keyword}")

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
        if msg.msgType == const_kuaishou.DATA_TYPE_TAG_KEYWORD:
            device = const_kuaishou.randomDevice()
            msg.msgData = urllib.quote_plus(urllib.unquote(msg.msgData))
            target = self.url.format(ud=device['ud'], did=device['did'])
            urlPack = urlprovider.UrlPack(priority=20, url=target)
            urlPack.setForm(self.form.format(keyword=msg.msgData, token=device['token']))
            urlPack.fillMsg(msg, self.pipe)
            urlPack.addKey('keyword', msg.msgData)
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
        log.debug("KuaiShouSearchTagProvider next: {}".format(urlPack))
        return urlPack

    def end(self):
        return False

    def check(self):
        return False

# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author xuke13@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年11月28日
#
################################################################################
"""
Created on 2017年11月28日

@author: xuke13
"""
from __future__ import division, print_function, unicode_literals
from libs import log
from libs.db import mongo
from libs import urlprovider
from libs.spider import signature
from libs.const import const_kuaishou
import urllib

#TOKEN = "16a02be9c89940068ee35dbc1d7780d6-753064385"
#TOKEN_CLIENT_SALT = "92647ba6ba3c8a362bd2c3440d0b6233"

class KuaiShouSearchUserProvider(urlprovider.UrlProvider):
    """
    classdocs
    """
    #登录时抓取的两个值
    #token和token_client_salt
    
    url = ("http://api.gifshow.com/rest/n/user/search?appver=5.4.4.302&"
           "did={did}&c=a&ver=5.4&ud={ud}&lon=116.2676372022031&lat=40.04144830295876&"
           "sys=ios11.1.2&mod=iPhone7%2C2&net=%E4%B8%AD%E5%9B%BD%E8%81%94%E9%80%9A_5")

    form = ("client_key=3c2cd3f3&country_code=cn&language=zh-Hans-CN%3Bq%3D1&"
            "order=desc&pcursor={pcursor}&token={token}&user_name={keyword}")
    
    #ext = {
    #    'token_client_salt': TOKEN_CLIENT_SALT
    #}
    
    def __init__(self):
        urlprovider.UrlProvider.__init__(self)
        self.db = mongo.DB()
        return

    def onReceiveMsg(self, msg):
        """
        :param msg:
        """
        log.debug("KuaiShouMainFeedProvider receive {}".format(msg))
        if msg.msgType == const_kuaishou.DATA_TYPE_KEYWORD:
            device = const_kuaishou.randomDevice()
            target = self.url.format(did=device['did'], ud=device['ud'])
            msg.msgData = urllib.quote_plus(urllib.unquote(msg.msgData))
            pcursor = msg.getKey('pcursor', 1)
            urlPack = urlprovider.UrlPack(priority=0, url=target)
            urlPack.setForm(self.form.format(pcursor=pcursor, keyword=msg.msgData, token=device['token']))
            urlPack.fillMsg(msg, self.pipe)
            urlPack.addKey('keyword', msg.msgData)
            urlPack.addKey('pcursor', pcursor)
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
        log.debug("KuaiShouSearchUserProvider next: {}".format(urlPack))
        return urlPack

    def end(self):
        return False

    def check(self):
        return False

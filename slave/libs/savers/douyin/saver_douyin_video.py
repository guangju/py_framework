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
import time
from libs.const import const_douyin
from libs import log
from libs.savers.douyin import saver_douyin_base
from libs.db import dbtools
from jsonpath_rw import parse

class DouyinVideoSaver(saver_douyin_base.DouyinSaverBase):
    
    
    def __init__(self, db=None):
        self.db = db
        self.xpath_actions = []
        self.register_action("$.aweme_list[*].video.play_addr.url_list", self.replace_https)
        self.register_action("$.aweme_list[*]", self.handleAwemeList)
        return
    
    def handleAwemeList(self, root, data, urlPack):  # @UnusedVariable
        if type(data) == dict:
            data = [data]
        for music in data:
            obj = dbtools.MongoObject()
            obj.setMeta(const_douyin.DATA_TYPE_VIDEO, const_douyin.DATA_PROVIDER, music["aweme_id"])
            obj.setData(music)
            obj.setUserId(music["author_user_id"])
            obj.save()
            log.debug("DouyinVideoSaver Inserting obj {}".format(obj.getLastObjectId()))
            self.addStatObject(obj.getLastObjectId(), const_douyin.DATA_TYPE_VIDEO)
        return
    
    def handleChallengeInfo(self, root, data, urlPack):  # @UnusedVariable
        log.debug(data)
        obj = dbtools.MongoObject()
        obj.setMeta(self.DATA_TYPE_TOPIC, self.DATA_PROVIDER, data["cid"])
        obj.setData(data)
        obj.save(self.MONGO_TABLE_TOPIC)
        return

    def replace_https(self, root, urls, urlPack):  # @UnusedVariable
        for index, url_o in enumerate(urls):
            if 'aweme.snssdk.com/aweme' in url_o:
                urls[index] = url_o.replace('https', 'http')

    # 补充扩展字段
    def add_extend_fileds(self,video):
        video['video_type'] = 3
        video['source_type'] = 1
        video['from_app'] = 'com.douyin'
        video['crawl_status'] = 0
        video['source'] = self.type_douyin
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        video['insert_time'] = cur_time
        video['update_time'] = cur_time    
    
    def preCheck(self, video, urlPack):
        exprs = [parse("$.aweme_list[*].aweme_id"),
                 parse("$.aweme_list[*].statistics.aweme_id"),
                 parse("$.aweme_list[*].video.play_addr.url_list"),
                 ]
        return self.checkTemplate(video, exprs, urlPack)
        
    
    
    
    
    
    
        
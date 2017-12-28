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
import json
import urllib
import urllib2
import traceback
from libs import log, conftool

mongo_server = 'ucp-table.spider.all.serv:8080'

if conftool.cur_env == 'mac':
    mongo_server = "10.62.239.27:8080"

mongo_db = 'mvideo'
mongo_pass = 'mvideo_1a2s3d4f'

def _req_get(url, username = mongo_db, password = mongo_pass):
    auth = urllib2.HTTPPasswordMgrWithDefaultRealm()
    auth.add_password(realm = None, uri = url, user = username, passwd = password)
    handler = urllib2.HTTPBasicAuthHandler(auth)
    opener = urllib2.build_opener(handler)
    urllib2.install_opener(opener)
    try:
        response = urllib2.urlopen(url)
        return response.read()
    except:
        traceback.print_exc()
        return None
    
    
def getByVideoId(videoId):
    for _ in range(10):
        try:
            col_videos = 'mvideo_meta'
            where = {}
            where['video_id'] = videoId
            where = json.dumps(where)
            where = urllib.quote(where)
            get_url_videos = 'http://' + mongo_server + '/get/' + col_videos + '?where=' + where + "&limit=10&order=@id"
            result = _req_get(get_url_videos)
            result = json.loads(result)
            result['data'] = json.loads(result['data'])
            return result
        except Exception as e:
            log.fatal("getByVideoId_error", e)
    return None

def isVideoCrawled(videoId):
    result = getByVideoId(videoId)
    if result is None:
        #异常, 默认为已抓取
        return 1
    if len(result['data']) == 0:
        return 0
    try:
        data = result['data']
        if data[0]['crawl'] == 0:
            return 0
        return 1
    except Exception as e:
        log.warning("getVideoCrawled", e)
        return 1
        

if __name__ == "__main__":
    vids = ["douyin_6464137546422029582", "huoshan_6485845010859166990", "kuaishou_791701338"]
    for vid in vids:
        #print(getByVideoId(vid))
        print(vid, isVideoCrawled(vid))
    
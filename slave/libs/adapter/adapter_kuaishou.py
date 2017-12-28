# -*- coding=utf-8 -*-
################################################################################
#
# rmb
# @author gonglixing@baidu.com
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# 2017年12月1日 上午1:47:11
#
################################################################################
"""
Created on 2017年12月1日

@author: gonglixing
"""
from __future__ import division, print_function, unicode_literals
from libs.adapter import adaptertool
import re
import json
from libs import log

emoji_pattern = re.compile(
    u"(\ud83d[\ude00-\ude4f])|"  # emoticons
    u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
    u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
    u"(\uD83E[\uDD00-\uDDFF])|"
    u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
    u"(\ud83c[\udde0-\uddff])|"  # flags (iOS)
    u"([\u2934\u2935]\uFE0F?)|"
    u"([\u3030\u303D]\uFE0F?)|"
    u"([\u3297\u3299]\uFE0F?)|"
    u"([\u203C\u2049]\uFE0F?)|"
    u"([\u00A9\u00AE]\uFE0F?)|"
    u"([\u2122\u2139]\uFE0F?)|"
    u"(\uD83C\uDC04\uFE0F?)|"
    u"(\uD83C\uDCCF\uFE0F?)|"
    u"([\u0023\u002A\u0030-\u0039]\uFE0F?\u20E3)|"
    u"(\u24C2\uFE0F?|[\u2B05-\u2B07\u2B1B\u2B1C\u2B50\u2B55]\uFE0F?)|"
    u"([\u2600-\u26FF]\uFE0F?)|"
    u"([\u2700-\u27BF]\uFE0F?)"
    "+", flags=re.UNICODE)

def remove_emoji(text):
    return emoji_pattern.sub(r'',text)

def remove_control_symbol(field):
    field = field.replace("\n","")
    field = field.replace("\r","")
    # field = field.replace(" ","")
    return field

def getVideoId(data):
    return {"video_id": str(data['photo_id'])}


def getTitle(data):  # @UnusedVariable
    return {"title": ""}


def getTime(data):
    return {"time": data['timestamp']}

def getImageUrl(data):
    if data['cover_thumbnail_urls'][0]['cdn'] == 'js2.a.yximgs.com':
        return {"image_url": data['cover_thumbnail_urls'][1]['url']}
    else:
        return {"image_url": data['cover_thumbnail_urls'][0]['url']}


def getPlayNum(data):
    return {"play_num": data['view_count']}


def getCommentNum(data):
    return {"comment_num": data['comment_count']}


def getForwardNum(data):
    return {"forward_num": data['forward_count']}


def getDislikeNum(data):
    return {"dislike_num": data['unlike_count']}


def getLikeNum(data):
    return {"like_num": data['like_count']}


def getAvatarSrc(data):
    return {"avatar_src": data['headurls'][0]['url']}


def getAuthorUserId(data):
    return {"author_user_id": str(data['user_id'])}


def getUploader(data):
    return {"uploader": remove_control_symbol(remove_emoji(data['user_name']))}


def getAuthorFan(data):
    if 'ownerCount' in data['_authorInfo_']:
        return {"author_fan": data['_authorInfo_']['ownerCount']['fan']}
    else:
        return {"author_fan": data['_authorInfo_']['userProfile']['ownerCount']['fan']}


def getAuthorFollow(data):
    if 'ownerCount' in data['_authorInfo_']:
        return {"author_follow": data['_authorInfo_']['ownerCount']['follow']}
    else:
        return {"author_follow": data['_authorInfo_']['userProfile']['ownerCount']['fan']}


def getAuthorDescription(data):
    if 'profile' in data['_authorInfo_']:
        return {"author_description": remove_control_symbol(remove_emoji(data['_authorInfo_']['profile']['user_text']))}
    else:
        return {"author_description": remove_control_symbol(remove_emoji(data['_authorInfo_']['userProfile']['profile']['user_text']))}

def getAuthorLocation(data): # @UnusedVariable
    return {"author_location": ''}


def getFromApp(data): # @UnusedVariable
    return {"from_app": "com.kuaishou"}


def getVideoType(data): # @UnusedVariable
    return {"video_type": 3}


def getSourceType(data): # @UnusedVariable
    return {"source_type": 1}


def getTargetUrl(data):
    if 'main_mv_urls' in data:
        return {"target_url": data['main_mv_urls'][0]['url']}
    return None

def getMicroVideoExt(data):
    microVideoExt = {}
    microVideoExt['microShareUrl'] = 'https://www.kuaishou.com/photo/' + str(data['user_id']) + '/' + str(data['photo_id'])
    microVideoExt['microAuthorUrl'] = 'http://m.kuaishou.com/user/' + str(data['user_id'])
    microVideoExt['caption'] = remove_control_symbol(remove_emoji(data['caption']))
    microVideoExt['tags'] = str(data['tags'])
    return {'microVideoExt': json.dumps(microVideoExt)}


def getVideoHW(data):
    return {'video_w': data['ext_params']['w'], 'video_h': data['ext_params']['h']}


def getCallBackUrl(data):
    return {'_callback_': data.get('_callback_')}


def getCrawl(data):
    return {'_crawl_': data.get('_crawl_', 0)}


def getAuthorWorks(data):
    if 'ownerCount' in data['_authorInfo_']:
        return {"author_works": data['_authorInfo_']['ownerCount']['photo_public']}
    else:
        return {"author_works": data['_authorInfo_']['userProfile']['ownerCount']['photo_public']}


## origin
def getNumVideos(data):
    return {"numVideos": data['ownerCount']['photo_public']}

def getNumFans(data):
    return {adaptertool.F_NUM_FANS: data['ownerCount']['fan']}

def getNumFollow(data):
    return {"numFollow": data['ownerCount']['follow']}

def getUserNick(data):
    return {"userName": data['profile']['user_name']}

def getUserDesc(data):
    return {"userDesc": data['profile']['user_text']}

def getUserId(data):
    return {adaptertool.F_UID: data['profile']['user_id']}

def getPlayCnt(data):
    return {"playCnt": data['view_count']}

def getTopicName(data):
    try:
        if "microVideoTopic" in data:
            return {"microVideoTopic": data['microVideoTopic']}
        return {"microVideoTopic": data['tag']}
    except:
        return {}

funcsAuthor = [getNumVideos, getNumFans, getNumFollow, getUserNick, getUserDesc, getUserId]

funcsVideoDetail = [getTargetUrl, getVideoId, getTitle, getTime, getImageUrl, getPlayNum,
                    getCommentNum, getForwardNum, getDislikeNum, getLikeNum, getAvatarSrc,
                    getAuthorUserId, getUploader, getAuthorFan, getAuthorFollow,
                    getAuthorDescription, getAuthorLocation, getFromApp, getVideoType,
                    getSourceType, getMicroVideoExt, getVideoHW, getCallBackUrl, getCrawl,
                    getAuthorWorks, getTopicName]

funcsTopicDetail = [getTopicName]

def transformAuthorDetail(data):
    resp = {}
    for f in funcsAuthor:
        r = f(data)
        resp.update(r)
    resp['3rdRawData'] = data
    return resp

def transfromVideoDetail(data):
    resp = {}
    for f in funcsVideoDetail:
        try:
            r = f(data)
            resp.update(r)
        except Exception as e:
            log.fatal("error_in_transfrom_video_detail, func={}, err={}, key={}".format(f, e, data.get('_key_')))
            raise e
    return resp


def transformTopicDetail(data):
    resp = {}
    for f in funcsTopicDetail:
        try:
            r = f(data)
            resp.update(r)
        except Exception as e:
            log.fatal("error_in_transfrom_topic_detail, func={}, err={}, key={}".format(f, e, data.get('_key_')))
            raise e
    return resp

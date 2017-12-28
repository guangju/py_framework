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
import time
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


def title_filter(title):
    while re.match('.*(\@[^(\\xe3\\x80\\x80) ]+)(\\xe3\\x80\\x80| ).*', title):
        title = title.replace(re.match('.*(\@[^(\\xe3\\x80\\x80) ]+)(\\xe3\\x80\\x80| ).*',title).group(1), '')
    while re.match('.*(\@[^$]+)$.*', title):
        title = title.replace(re.match('.*(\@[^$]+)$.*', title).group(1), '')
    title.replace('抖音小助手', '')
    title.replace('抖音公民', '')
    title = remove_control_symbol(remove_emoji(title))
    title = ' '.join(title.split())
    if title == '' or title == ' ' or '抖音' in title or '原声' in title:
        title = '分享视频'
    return title


def getVideoId(data):
    return {"video_id": str(data['aweme_id'])}


def getTitle(data):  # @UnusedVariable
    return {"title": title_filter(data['desc'])}


def getTime(data):
    return {"time": data['create_time']}


def getImageUrl(data):
    return {"image_url": data['video']['origin_cover']['url_list'][0]}


def getPlayNum(data):
    return {"play_num": data['statistics']['play_count']}


def getCommentNum(data):
    return {"comment_num": data['statistics']['comment_count']}


def getForwardNum(data):
    return {"forward_num": data['statistics']['share_count']}


def getLikeNum(data):
    return {"like_num": data['statistics']['digg_count']}


def getAvatarSrc(data):
    if '_authorInfo_' in data:
        return {"avatar_src": data['_authorInfo_']['avatar_larger']['url_list'][0]}
    return {"avatar_src": data['author']['avatar_larger']['url_list'][0]}


def getAuthorUserId(data):
    return {"author_user_id": str(data['author']['uid'])}


def getUploader(data):
    return {"uploader": remove_control_symbol(remove_emoji(data['author']['nickname']))}


def getAuthorFan(data):
    return {"author_fan": data['_authorInfo_']['follower_count']}


def getAuthorFollow(data):
    return {"author_follow": data['_authorInfo_']['following_count']}


def getAuthorWorks(data):
    return {"author_works": data['_authorInfo_']['aweme_count']}


def getAuthorDescription(data):
    return {"author_description": remove_control_symbol(remove_emoji(data['_authorInfo_']['signature']))}


def getAuthorBackgroundPic(data):
    return {"author_background_pic": data['author']['avatar_medium']['url_list'][0]}


def getAuthorAge(data):
    return {"author_age": data['_authorInfo_']['birthday']}


def getAuthorLocation(data): # @UnusedVariable
    return {"author_location": data['_authorInfo_'].get('location', '')}


def getFromApp(data): # @UnusedVariable
    return {"from_app": "com.douyin"}


def getVideoType(data): # @UnusedVariable
    return {"video_type": 3}


def getSourceType(data): # @UnusedVariable
    return {"source_type": 1}


def getTargetUrl(data):
    for url in data['video']['play_addr']['url_list']:
        if 'aweme.snssdk.com/aweme' in url:
            return {'target_url': url.replace('https', 'http')}
    return {'target_url': data['video']['play_addr']['url_list'][0].replace('https','http')}


def getMicroVideoExt(data):
    if 'music' not in data:
        return {}
    microVideoExt = {}
    microVideoExt['musicTitle'] = remove_control_symbol(remove_emoji(data['music']['title']))
    microVideoExt['microShareUrl'] = 'https://www.douyin.com/share/video/' + str(data['aweme_id']) + '/'
    microVideoExt['microAuthorUrl'] = 'https://www.douyin.com/share/user/' + str(data['author']['uid']) + '/'
    microVideoExt['region'] = data['region']
    microVideoExt['s2cTime'] = int(time.time())
    return {'microVideoExt': json.dumps(microVideoExt)}


def getVideoHw(data): # @UnusedVariable
    return {"video_w": data['video'].get('width', ''), 'video_w': data['video'].get('height', '')}


def getCallBackUrl(data):
    return {'_callback_': data.get('_callback_')}


def getCrawl(data):
    return {'_crawl_': data.get('_crawl_', 0)}
## origin


def getNumVideos(data):
    return {"numVideos": data['stats']['item_count']}


def getNumFans(data):
    return {adaptertool.F_NUM_FANS: data['stats']['follower_count']}


def getNumFollow(data):
    return {adaptertool.F_NUM_FOLLOWING: data['stats']['following_count']}


def getUserNick(data):
    return {"userName": data['nickname']}


def getUserDesc(data):
    return {"userDesc": data['signature']}


def getUserId(data):
    return {adaptertool.F_UID: data['id_str']}


def getPlayCnt(data):
    return {"playCnt": data['view_count']}

def getTopicName(data):
    try:
        if "microVideoTopic" in data:
            return {"microVideoTopic": data['microVideoTopic']}
        return {"microVideoTopic": data['cha_name']}
    except:
        return {}

funcsAuthor = [getNumVideos, getNumFans, getNumFollow, getUserNick, getUserDesc, getUserId]


funcsVideoDetail = [getTargetUrl, getVideoId, getTitle, getTime, getImageUrl, getPlayNum, getCommentNum, getForwardNum, getLikeNum, getAvatarSrc,getAuthorBackgroundPic, getAuthorUserId, getUploader, getAuthorFan, getAuthorFollow, getAuthorDescription, getAuthorLocation, getFromApp, getVideoType,getSourceType, getMicroVideoExt, getAuthorWorks, getAuthorAge, getCallBackUrl, getCrawl, getTopicName]

funcsTopicDetail = [getTopicName]

def transformAuthorDetail(data):
    resp = {}
    for f in funcsAuthor:
        try:
            r = f(data)
            resp.update(r)
        except Exception as e:
            log.fatal(f, e)
    resp['3rdRawData'] = data
    return resp


def transfromVideoDetail(data):
    resp = {}
    if '_authorInfo_' in data and 'user' in data['_authorInfo_']:
        data['_authorInfo_'].update(data['_authorInfo_']['user'])
    if 'author' not in data:
        data['author'] = data['_authorInfo_']
        
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

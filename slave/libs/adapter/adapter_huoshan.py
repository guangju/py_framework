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
    return {"video_id": str(data['id'])}


def getTitle(data):  # @UnusedVariable
    return {"title": remove_control_symbol(remove_emoji(data['title']))}


def getTime(data):
    return {"time": data['create_time']}


def getImageUrl(data):
    return {"image_url": data['video']['cover']['url_list'][0]}


def getPlayNum(data):
    return {"play_num": data['stats']['play_count']}


def getCommentNum(data):
    return {"comment_num": data['stats']['comment_count']}


def getForwardNum(data):
    return {"forward_num": data['stats']['share_count']}


def getVideoHotScore(data):
    try:
        return {"video_hot_score": int(data['tips'].split(':')[1].replace(' ',''))}
    except:
        return {"video_hot_score": 0}

def getLikeNum(data):
    return {"like_num": data['stats']['digg_count']}


def getAvatarSrc(data):
    if '_authorInfo_' in data:
        return {"avatar_src": data['_authorInfo_']['avatar_jpg']['url_list'][0]}
    return {"avatar_src": data['author']['avatar_jpg']['url_list'][0]}


def getAuthorUserId(data):
    if '_authorInfo_' in data:
        return {"author_user_id": str(data['_authorInfo_']['id'])}
    return {"author_user_id": str(data['author']['id'])}


def getUploader(data):
    if '_authorInfo_' in data:
        return {"uploader": remove_control_symbol(remove_emoji(data['_authorInfo_']['nickname']))}
    return {"uploader": remove_control_symbol(remove_emoji(data['author']['nickname']))}


def getAuthorFan(data):
    return {"author_fan": data['_authorInfo_']['stats']['follower_count']}


def getAuthorFollow(data):
    return {"author_follow": data['_authorInfo_']['stats']['following_count']}


def getAuthorWorks(data):
    return {"author_works": data['_authorInfo_']['stats']['item_count']}


def getAuthorDescription(data):
    if '_authorInfo_' in data:
        return {"author_description": remove_control_symbol(remove_emoji(data['_authorInfo_']['signature']))}
    return {"author_description": remove_control_symbol(remove_emoji(data['author']['signature']))}


def getAuthorBackgroundPic(data):
    return {"author_background_pic": data['author']['avatar_jpg']['url_list'][0]}


def getAuthorAge(data):
    if '_authorInfo_' in data:
        return {"author_age": data['_authorInfo_']['birthday_description']}
    return {"author_age": data['author']['birthday_description']}


def getAuthorLocation(data): # @UnusedVariable
    if '_authorInfo_' in data:
        return {"author_location": data['_authorInfo_']['city']}
    return {"author_location": data['author']['city']}


def getFromApp(data): # @UnusedVariable
    return {"from_app": "com.huoshan"}


def getVideoType(data): # @UnusedVariable
    return {"video_type": 3}


def getSourceType(data): # @UnusedVariable
    return {"source_type": 1}


def getTargetUrl(data):
    for url in data['video']['url_list']:
        if 'api.huoshan.com/hotsoon' in url:
            return {'target_url': url.replace('https', 'http')}
    return {'target_url': data['video']['url_list'][1].replace('https','http')}


def getMicroVideoExt(data):
    microVideoExt = {}
    microVideoExt['microShareUrl'] = 'https://www.huoshan.com/share/video/' + str(data['id']) + '/'
    if '_authorInfo_' in data:
        microVideoExt['microAuthorUrl'] = 'https://www.huoshan.com/share/user/' + str(data['_authorInfo_']['id']) + '/'
    else:
        microVideoExt['microAuthorUrl'] = 'https://www.huoshan.com/share/user/' + str(data['author']['id']) + '/'
    microVideoExt['region'] = data['location']
    return {'microVideoExt': json.dumps(microVideoExt)}



def getVideoHw(data): # @UnusedVariable
    return {"video_w": data['video'].get('width', ''), 'video_w': data['video'].get('height', '')}


def getCallBackUrl(data):
    return {'_callback_': data.get('_callback_')}


def getCrawl(data):
    return {'_crawl_': data.get('_crawl_', 0)}

## origin
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

funcsAuthor = [getNumVideos, getNumFans, getNumFollow, getUserNick, getUserDesc, getUserId]

funcsVideoDetail = [getTargetUrl, getVideoId, getTitle, getTime, getImageUrl, getPlayNum,getCommentNum, getForwardNum, getVideoHotScore, getLikeNum, getAvatarSrc,getAuthorBackgroundPic, getAuthorUserId, getUploader, getAuthorFan, getAuthorFollow, getAuthorDescription, getAuthorLocation, getFromApp, getVideoType, getSourceType, getMicroVideoExt, getAuthorWorks, getAuthorAge, getCallBackUrl, getCrawl]

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
    if 'author' not in data and '_authorInfo_' in data:
        data['author'] = data['_authorInfo_']
    resp = {}
    for f in funcsVideoDetail:
        try:
            r = f(data)
            resp.update(r)
        except Exception as e:
            log.fatal("error_in_transfrom_video_detail, func={}, err={}, key={}".format(f, e, data.get('_key_')))
            raise e
    return resp

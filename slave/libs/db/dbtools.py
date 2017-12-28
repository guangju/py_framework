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
from libs.db import mongo
from libs.const import const
from libs import util
import time
import pymongo

def gen_object_id(dataType, dataSource, thirdId, version=1):
    '''
    
    :param dataType:
    :param dataSource:
    :param thirdId:
    :param version:
    '''
    concat = gen_object_key(dataType, dataSource, thirdId, version)
    resp = util.md5(concat)
    return resp, concat

def gen_object_key(dataType, dataSource, thirdId, version=1):
    concat = "{}-{}-{}-{}".format(str(dataType).strip(),
                                  str(dataSource).strip(),
                                  str(thirdId).strip(),
                                  str(version).strip()
                                  )
    return concat

def get_object_id_by_key(key):
    return util.md5(key)

def get_key_info(key):
    return key.split("-")

class MongoObject(object):
    """
    classdocs
    """


    def __init__(self, db=None):
        """
        Constructor
        """
        if db is None:
            db = mongo.DB()
        self.db = db
        self.dataSource = ''
        self.dataType = ''
        self.user3rdId = None
        self.video3rdId = None
        self.topic3rdId = None
        self.data = {}
        return
    
    def setMeta(self, dataType, dataSource, thirdId, version=1):
        '''
        
        :param dataType:
        :param dataSource:
        :param thirdId:
        :param version:
        '''
        self.objId, self.key = gen_object_id(dataType, dataSource, thirdId, version)
        self.dataSource = dataSource
        if dataType == const.DATA_TYPE_AUTHOR:
            self.user3rdId = thirdId
        elif dataType == const.DATA_TYPE_TOPIC:
            self.topic3rdId = thirdId
        elif dataType == const.DATA_TYPE_VIDEO:
            self.video3rdId = thirdId
        self.dataType = dataType
    
    def setUserId(self, thirdUid):        
        self.user3rdId = str(thirdUid)

    def setTopicId(self, topicId):
        self.topic3rdId = str(topicId)

    def setData(self, data):
        '''
        
        :param data:
        '''
        self.data = data
    
    def save(self, table=None):
        '''
        
        :param table:
        '''
        val = {
            '_key_': self.key,
            '_utime_': int(time.time()),
            '_dataSource_': self.dataSource
            }
        #适配字段
        if self.user3rdId is not None:
            self.data["_user3rdId_"] = str(self.user3rdId)
        if self.video3rdId is not None:
            self.data["_video3rdId_"] = str(self.video3rdId)
        if self.topic3rdId is not None:
            self.data['_topic3rdId_'] = str(self.topic3rdId)
            
        val.update(self.data)
        insertVal = {}
        insertVal["_insertTime_"] = int(time.time())
        insertVal["_crawl_"] = 0
        if table is None:
            table = const.getTable(self.dataType)
        self.db.updateByKey(table, self.key, val, insertVal)
        
    def getLastObjectId(self):
        """
        get last insert objectId
        """
        return self.objId


class MongoIterator(object):
    
    BATCH = 1000
    def __init__(self, table, condition, fields=None):
        self.condition = condition
        self.table = table
        self.fields = fields
        self.db = mongo.DB()
        self.lastId = None
    
    def nextBatch(self):
        if self.lastId is None:
            cursor = self.db.getCollection(self.table).find(self.condition, self.fields).sort([("_id", pymongo.ASCENDING)]).limit(self.BATCH)
        else:
            cond = self.condition
            cond['_id'] = {'$gt': self.lastId}
            cursor = self.db.getCollection(self.table).find(cond, self.fields).sort([("_id", pymongo.ASCENDING)]).limit(self.BATCH)
        result = [x for x in cursor]
        if len(result) == 0:
            return False
        self.lastId = result[-1]['_id']
        return result
            
            
if __name__ == "__main__":
    mi = MongoIterator("m_author", {"_dataSource_":"douyin"}, {"_key_": 1})
    ts = time.time()
    cnt = 0
    s = set()
    while True:
        r = mi.nextBatch()
        if r is False:
            break
        cnt += len(r)
        for item in r:
            s.add(item['_key_'])
        if cnt >= 10000:
            break
        
    te = time.time()
    assert(len(s) == cnt)
    print("took: {}".format(te - ts))
    
        
    
            
            
        
    
        
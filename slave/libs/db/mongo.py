#coding=utf-8
from __future__ import division, print_function, unicode_literals
from pymongo.mongo_client import MongoClient
from libs import conftool, util
import time
from libs.const import const
# MONGODB_HOST = '10.151.30.34'
# MONGODB_PORT = '27000'

MONGODB_HOST = conftool.cf.get("mongo", "host")
MONGODB_PORT = conftool.cf.get("mongo", "port")


#MONGODB_HOST = '10.99.53.31'
#MONGODB_PORT = '8386'

#TABLE_TOPIC = 'mTopics'
#TABLE_TOPIC_VIDEOS = 'mTopicsToVideos'

class DB(object):

    def __init__(self, db='micro_video', host=MONGODB_HOST, port=MONGODB_PORT):
        """
        
        :param db:
        :param host:
        :param port:
        """
        self.host = host
        self.port = port
        self.db = db
        self.client = MongoClient("mongodb://%s:%s/" % (self.host, self.port))
        self.db = self.client[db]
    
    def getOne(self, table, objectId, key="_id"):
        """
        
        :param table:
        :param objectId:
        """
        resp = self.db[table].find({key: str(objectId)})
        if resp.count() > 0:
            return resp[0]
        return None
        
    def isItemUpdatedRecently(self, itemKey, recentSeconds=3*86400):
        return self.isObjectUpdatedRecently(const.getTable(itemKey), itemKey, recentSeconds)
        
    def isObjectUpdatedRecently(self, table, key, recentSeconds=3600):
        data = self.getOne(table, key, '_key_')
        if data is None:
            return False
        return data.get('_utime_') >= time.time() - recentSeconds
    
    def find(self, table, condition, limit=-1):
        if limit > 0:
            cursor = self.db[table].find(condition).limit(limit)
        else:
            cursor = self.db[table].find(condition)
        return [x for x in cursor]
    
    def getCollection(self, coll):
        return self.db[coll]
    
    def update(self, table, objectId, value, setOnInsert=None):
        """
        
        :param table:
        :param objectId:
        :param value:
        :param setOnInsert:
        """
        if setOnInsert is None:
            return self.db[table].update({"_id": objectId}, {'$set': value}, upsert=True)
        return self.db[table].update({"_id": objectId}, {'$set': value, '$setOnInsert': setOnInsert}, upsert=True)
     
    def updateByKey(self, table, key, value, setOnInsert=None):
        """
        
        :param table:
        :param objectId:
        :param value:
        :param setOnInsert:
        """
        if setOnInsert is None:
            return self.db[table].update({"_key_": key, "_id": util.md5(key)}, {'$set': value}, upsert=True)
        return self.db[table].update({"_key_": key, "_id": util.md5(key)}, {'$set': value, '$setOnInsert': setOnInsert}, upsert=True)
        
   

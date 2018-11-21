#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : simulate_redis.py.py
# @Author: Lvious
# @Date  : 2018/11/15
# @Desc  :

import redis

class TweetSource:
    def __int__(self):
        print('initating TweetSource')
    def setReaderFromFile(self,filename):
        try:
            self._fp = open(file=filename,mode='rb')
        except Exception as e:
            print("file can\'t open:")
            print(e)
    def setSenderRedis(self,host,port):
        self._redis = redis.Redis(host,port)
    def reader(self):
        row = None
        try:
            row = self._fp.readline()
        except Exception as e:
            print('reader error:')
            raise(e)
        return row
    def send2Redis(self,key,item):
        try:
            self._redis.rpush(key,item)
        except Exception as e:
            print("send error:")
            print(e)
if __name__=="__main__":
    HOST = "58.198.176.122"
    PORT = 7379
    KEY  = "20171030"
    filePath = "D:/Datasets/tweet-stream_06.json"
    tweetSource =  TweetSource()
    tweetSource.setReaderFromFile(filePath)
    tweetSource.setSenderRedis(HOST,PORT)
    count=0
    while True:
        item = tweetSource.reader()
#         time.sleep(0.03)
        if item:
            count+=1
            if count%1000==0:
                print(count)
            tweetSource.send2Redis(KEY,item)
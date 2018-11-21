#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : RedisClient.py
# @Author: Lvious
# @Date  : 2018/11/12
# @Desc  :

import redis
from DB.DBActionsInterface import DBActionsInterface

class RedisClient(DBActionsInterface):
    def __init__(self,**kwargs):
        self.dbHost = kwargs['dbHost']
        self.dbPort = kwargs['dbPort']
        self.dbName = kwargs['dbName']
        self.key = kwargs
        self.conn = redis.Redis(self.dbHost,self.dbPort)

    def fetch(self):
        pass

    def fetchAll(self):
        pass

    def insert(self):
        pass

    def insertBatch(self):
        pass

    def update(self):
        pass

    def delete(self):
        pass

    def deleteBatch(self):
        pass

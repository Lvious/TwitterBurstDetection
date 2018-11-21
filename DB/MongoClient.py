#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : MongoClient.py
# @Author: Lvious
# @Date  : 2018/11/12
# @Desc  :

import pymongo

class MongoClient(object):
    def __init__(self,**kwargs):
        self.dbHost = kwargs['dbHost']
        self.dbPort = kwargs['dbPort']
        self.dbName = kwargs['dbName']
        self.__conn = pymongo.MongoClient(self.dbHost,self.dbPort)
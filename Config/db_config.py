#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : db_config.py
# @Author: Lvious
# @Date  : 2018/11/12
# @Desc  :
from Config.parse_config import parse_config
from Utils.util_class import LazyProperty

class DBConfig(object):
    def __init__(self):
        self.config = parse_config()

    @LazyProperty
    def dbType(self):
        return self.config.get('db','type')

    @LazyProperty
    def dbHost(self):
        return self.config.get('db','host')

    @LazyProperty
    def dbPort(self):
        return self.config.get('db', 'port')

    @LazyProperty
    def dbUser(self):
        return self.config.get('db', 'user')

    @LazyProperty
    def dbPassword(self):
        return self.config.get('db', 'password')

    @LazyProperty
    def dbName(self):
        return self.config.get('db','dbname')

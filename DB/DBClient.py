#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : DBClient.py
# @Author: Lvious
# @Date  : 2018/11/12
# @Desc  : DB tools

''' 尝试编写抽象工厂类 '''
'''抽象工厂最大的优点是抽象生产实例（产品）的类（工厂）的共性特征，根据实际需要实例化类（工厂）来生产对应的产品实例'''
from Config.db_config import DBConfig


class DBClient(object):
    def __init__(self):
        self.config = DBConfig()
        self.__initDBClient()
    '''简单工厂：返回指定类型的构造方法，需要判断指定工厂，违背了依赖倒置原则'''
    '''解决方法：采用反射机制，无侵入式可扩展生产工厂类型，缺点是无法检测检查自定义工厂类型。可能会有风险'''
    def __initDBClient(self):
        self.__type = None
        if "redis"==self.config.dbType:
            self.__type = "RedisClient"
        elif "mongo"==self.config.dbType:
            self.__type = "MongoClient"
        elif "csv"==self.config.dbType:
            self.__type = "CSVClient"
        else:
            pass
        assert self.__type,"not suport DB type %s".format(self.__type)
        self.client = getattr(__import__("DB."+self.__type,fromlist=True),self.__type)(dbHost = self.config.dbHost,
                                                                   dbPort = self.config.dbPort,
                                                                   dbName = self.config.dbName,
                                                                   dbUser = self.config.dbUser,
                                                                   dbPassword = self.config.dbPassword
                                                                   )
    '''具体操作如何处理：抽象一个操作接口？将产品的操作封装到操作接口中'''
    def fetch(self,key,**kwargs):
        return self.client.fetch(key,**kwargs)

    def fetchAll(self,key,**kwargs):
        return self.client.fetchAll(key,**kwargs)

    def insert(self,key,value,**kwargs):
        try:
            rt = self.insert(key,value)
        except Exception as e:
            print(e)
        return rt

    def update(self,key,value,**kwargs):
        return self.update(key,value,**kwargs)

    def delete(self,key,**kwargs):
        return self.client.delete(key,**kwargs)


if __name__ == '__main__':
    db = DBClient()
    print(db.__dict__)
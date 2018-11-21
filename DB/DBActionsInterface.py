#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : DBUtils.py
# @Author: Lvious
# @Date  : 2018/11/14
# @Desc  :
'''具体DB产品的抽象类，将操作抽象成接口，利用python的abc模块进行抽象类的定义，该模块只能被继承，不能被实例化'''

import abc

class DBActionsInterface(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def fetch(self):
        pass

    @abc.abstractmethod
    def fetchAll(self):
        pass

    @abc.abstractmethod
    def insert(self):
        pass

    @abc.abstractmethod
    def insertBatch(self):
        pass

    @abc.abstractmethod
    def update(self):
        pass

    @abc.abstractmethod
    def delete(self):
        pass

    @abc.abstractmethod
    def deleteBatch(self):
        pass

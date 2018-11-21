#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : util_class.py
# @Author: Lvious
# @Date  : 2018/11/13
# @Desc  :

class LazyProperty(object):
    """
    LazyProperty
    explain: http://www.spiderpy.cn/blog/5/
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, owner):
        if instance is None:
            return self
        else:
            value = self.func(instance)
            setattr(instance, self.func.__name__, value)
            return value

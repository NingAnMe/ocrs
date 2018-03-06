# coding:utf-8
"""
pm_time.py
时间处理相关函数
~~~~~~~~~~~~~~~~~~~
creation time : 2018 1 19
author : anning
~~~~~~~~~~~~~~~~~~~
"""

import os
import sys
import logging
import re
import time
from datetime import datetime
from functools import wraps
from contextlib import contextmanager
from posixpath import join

import h5py
import numpy as np
from configobj import ConfigObj


def get_ymd_and_hm(file_path):
    """
    从文件名或者目录名中获取日期和时间
    :param file_path: (str)文件或者目录的路径
    :return: 存放年月日和小时分钟的列表
    """
    if file_path:
        file_name = os.path.split(file_path)[1]
    else:
        raise ValueError('value error: wrong file_path')
    # 从文件名中获取
    if os.path.isfile(file_path):
        pat = r'.*(\d{8})_(\d{4})'
        m = re.match(pat, file_name)
        ymd = m.group(1)
        hm = m.group(2)
        return [ymd, hm]
    # 从目录名中获取
    elif os.path.isdir(file_path):
        pat = r'.*(\d*)'
        m = re.match(pat, file_name)
        ymd = m.group()[0:8]
        hm = m.group()[8:12]
        return [ymd, hm]


def is_cross_time(start_date1, end_date1, start_date2, end_date2):
    """
    判断俩个时间段是否有交叉
    :param start_date1: (datetime)第一个时间范围的开始时间
    :param end_date1: (datetime)第一个时间范围的结束时间
    :param start_date2: (datetime)第二个时间范围的开始时间
    :param end_date2: (datetime)第二个时间范围的结束时间
    :return: 布尔值
    """
    if start_date2 <= start_date1 <= end_date2:
        return True
    elif start_date2 <= end_date1 <= end_date2:
        return True
    elif start_date2 >= start_date1 and end_date2 <= end_date1:
        return True
    else:
        return False


def str2date(date):
    """
    将字符串日期转换为 datetime
    :param date: (str) YYYYMMDD 或者 YYYYMM 或者 YYYY
    :return:
    """
    y = date[0:4]
    m = date[4:6]
    d = date[6:8]
    if y:
        y = int(y)
        if m:
            m = int(m)
            if d:
                d = int(d)
                date_time = datetime(y, m, d)
                return date_time
            else:
                d = 1
                date_time = datetime(y, m, d)
                return date_time
        else:
            m = 1
            d = 1
            date_time = datetime(y, m, d)
            return date_time
    else:
        raise ValueError()


def date_str2list(date_range):
    """
    将字符串格式的时间范围转换为一个列表
    :param date_range: (str) YYYYMMDD-YYYYMMDD 或者 YYYYMM-YYYYMM 或者 YYYY-YYYY
    :return: (list)
    """
    d = date_range.split('-')
    date_range = [i for i in d]
    return date_range


def get_date_range(date_range):
    """
    将字符串格式的时间范围转换为 datetime
    :param date_range: (str) YYYYMMDD-YYYYMMDD 或者 YYYYMM-YYYYMM 或者 YYYY-YYYY
    :return: (list)存放开始日期和结束日期
    """
    start_date, end_date = date_str2list(date_range)
    start_date = str2date(start_date)
    end_date = str2date(end_date)
    return [start_date, end_date]


def get_dsl(filename, launch_date):
    """
    根据文件名和发射时间获取相差的天数
    :param filename: (str)文件名
    :param launch_date: (str)卫星发射时间 YYYYMMDD
    :return: (int)
    """
    ymd, hm = get_ymd_and_hm(filename)
    date1 = str2date(ymd)
    date2 = str2date(launch_date)
    delta = date1 - date2
    dsl = delta.days
    return dsl


def time_this(func):
    """
    装饰器，测试函数的运行时间
    :param func:
    :return:
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.clock()
        r = func(*args, **kwargs)
        end = time.clock()
        print('{}.{} : {}'.format(func.__module__, func.__name__, end - start))
        return r
    return wrapper


@contextmanager
def time_block(label):
    start = time.clock()
    try:
        yield
    finally:
        end = time.clock()
        print(u'{} : {}'.format(label, end - start))

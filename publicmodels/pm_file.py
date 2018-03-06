# coding:utf-8
"""
pm_file.py
目录和文件相关函数
~~~~~~~~~~~~~~~~~~~
creation time : 2018 1 19
author : anning
~~~~~~~~~~~~~~~~~~~
"""

import os
import sys
import logging
import re
from datetime import datetime, timedelta
from posixpath import join

import h5py
import numpy as np
from configobj import ConfigObj

from pm_time import get_ymd_and_hm, is_cross_time, get_date_range, str2date


class File(object):

    def __init__(self, path, name=None, suffix=None, date=None, time=None):
        """
        :param path: 文件路径
        :param name: 文件名
        :param suffix: 文件后缀
        :param date: 文件中数据的创建日期
        :param time: 文件中数据的创建时间
        """
        self.path = path
        self.name = name
        self.suffix = suffix
        self.date = date
        self.time = time

    def get_status(self):
        pass

    def get_name(self):
        """
        获取路径中的文件名
        :return:
        """
        file_dir, name = os.path.split(self.path)
        self.name = name

    def get_suffix(self):
        pass

    def get_date(self):
        pass

    def get_time(self):
        pass


def get_file_list(dir_path, pattern=r'.*'):
    """
    查找目录下的所有符合匹配模式的文件的绝对路径，包括文件夹中的文件
    :param dir_path: (str)目录路径
    :param pattern: (str)匹配模式 'hdf'
    :return: (list) 绝对路径列表
    """
    file_list = []
    # 递归查找目录下所有文件
    for root, dir_list, file_names in os.walk(dir_path):
        for i in file_names:
            m = re.match(pattern, i)
            if m:
                file_list.append(os.path.join(root, i))
    return file_list


def get_path_and_name(file_path):
    """
    通过一个绝对地址获取文件的所在文件夹和文件名
    :param file_path: (str)文件的完整路径名
    :return: (list)[路径, 文件名]
    """
    if os.path.isfile(file_path):
        path, file_name = os.path.split(file_path)
        return [path, file_name]
    else:
        raise ValueError('value error: not a file_path')


def filter_file_list(file_list, pattern=r'.*'):
    """
    过滤符合匹配模式的文件
    :param file_list: (list) 存放文件名的列表
    :param pattern: (str) 匹配规则
    :return:
    """
    new_file_list = []
    for file_name in file_list:
        m = re.match(pattern, file_name)
        if m:
            new_file_list.append(file_name)
    return new_file_list


def filter_dir_by_date_range(dir_path, start_date, end_date):
    """
    过滤日期范围内的目录
    :return:
    """
    dirs = os.listdir(dir_path)
    tem_dir_list = []
    for dir_name in dirs:
        dir_date = int(dir_name)
        if len(dir_name) == 4:
            start_date_tem = int(start_date.strftime('%Y'))
            end_date_tem = int(end_date.strftime('%Y'))
        elif len(dir_name) == 6:
            start_date_tem = int(start_date.strftime('%Y%m'))
            end_date_tem = int(end_date.strftime('%Y%m'))
        elif len(dir_name) == 8:
            start_date_tem = int(start_date.strftime('%Y%m%d'))
            end_date_tem = int(end_date.strftime('%Y%m%d'))
        else:
            raise ValueError('value error: dir_path')

        if is_cross_time(dir_date, dir_date, start_date_tem, end_date_tem):
            tem_dir_list.append(os.path.join(dir_path, dir_name))
    return tem_dir_list

# coding:utf-8
"""
pm_main.py
主程序相关函数
~~~~~~~~~~~~~~~~~~~
creation time : 2018 1 19
author : anning
~~~~~~~~~~~~~~~~~~~
"""

import os
import sys
import logging
import re
from datetime import datetime
from posixpath import join

import h5py
import numpy as np
from configobj import ConfigObj


def get_config(file_path, file_name):
    """
    获取配置信息
    :param file_path: (str)配置文件目录
    :param file_name: (str)文件名
    :return: (configobj)
    """
    config_file = join(file_path, file_name)
    if os.path.isfile(config_file):
        config_obj = ConfigObj(config_file)
        return config_obj
    else:
        raise ValueError('配置文件不存在')

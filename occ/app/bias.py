#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/7/24 10:03
@Author  : AnNing
"""
import numpy as np


class Bias(object):
    def __init__(self):
        """
        data1 - data2 或者 data1/data2 - 1
        :param data1:
        :param data2:
        """
        self.error = False

    @staticmethod
    def absolute_deviation(data1, data2):
        """
        求绝对偏差 data1 - data2
        :return:
        """
        return data1 - data2

    @staticmethod
    def relative_deviation(data1, data2):
        """
        求相对偏差 data1/data2 - 1
        :return:
        """
        return data1 / data2 - 1

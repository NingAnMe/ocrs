#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/6/8 10:48
@Author  : AnNing
"""

import os
from PB.pb_io import Config


class InitApp():
    """
    加载全局配置文件
    """
    def __init__(self, sat_sensor):
        """
        初始化
        """
        self.error = False

        self.main_path = os.path.dirname(os.path.realpath(__file__))
        self.config_path = os.path.join(self.main_path, "cfg")
        self.global_config_file = os.path.join(self.config_path, "global.cfg")
        self.sat_config_file = os.path.join(self.config_path, "{}.yaml".format(sat_sensor))

        self.global_config = Config(self.global_config_file)
        self.sat_config = Config(self.sat_config_file)
        if len(self.global_config) == 0 or len(self.sat_config) == 0:
            self.error = True

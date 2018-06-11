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

        # 初始化路径
        self.app_path = os.path.dirname(os.path.realpath(__file__))
        self.main_path = os.path.dirname(self.app_path)
        self.config_path = os.path.join(self.main_path, "cfg")
        self.global_config_file = os.path.join(self.config_path, "global.cfg")
        self.sat_config_file = os.path.join(self.config_path, "{}.yaml".format(sat_sensor))

        # 读取配置文件
        self.global_config = Config(self.global_config_file)
        self.sat_config = Config(self.sat_config_file)
        if self.global_config.error or self.sat_config.error:
            self.error = True

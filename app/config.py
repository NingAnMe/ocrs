#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/6/8 10:48
@Author  : AnNing
"""

from PB.pb_io import Config


class GlobalConfig(Config):
    """
    加载全局配置文件
    """

    def __init__(self, config_file):
        """
        初始化
        """
        Config.__init__(self, config_file)
        self.error = False

        self.load_cfg_file()
        # 添加需要的配置信息
        try:
            # ###################IN#######################
            # 原始文件路径
            self.l1_in_path = self.config_data['PATH']['IN']['l1']
            self.obc_in_path = self.config_data['PATH']['IN']['obc']
            self.ncep_in_path = self.config_data['PATH']['IN']['ncep']
            self.coeff_in_path = self.config_data['PATH']['IN']['coeff']

            # ###################MID#######################
            # 中间文件的储存路径
            self.calibrate_mid_path = self.config_data['PATH']['MID']['calibrate']
            self.incfg_mid_path = self.config_data['PATH']['MID']['incfg']
            self.ncep_mid_path = self.config_data['PATH']['MID']['ncep']
            self.granule_mid_path = self.config_data['PATH']['MID']['granule']
            self.projection_mid_path = self.config_data['PATH']['MID']['projection']

            # ###################OUT#######################
            # 输出的产品的存放路径
            self.daily_out_path = self.config_data['PATH']['OUT']['daily']
            self.day_10_out_path = self.config_data['PATH']['OUT']['day_10']
            self.monthly_out_path = self.config_data['PATH']['OUT']['monthly']
            self.quarterly_out_path = self.config_data['PATH']['OUT']['quarterly']
            self.yearly_out_path = self.config_data['PATH']['OUT']['yearly']
            self.log_out_path = self.config_data['PATH']['OUT']['log']
        except Exception as why:
            print why
            self.error = True
            print "Load config file error: {}".format(self.config_file)

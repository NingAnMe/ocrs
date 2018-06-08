#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/6/8 10:48
@Author  : AnNing
"""


class GlobalConfig():
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
            self.l1_in_path = self.config_data['PATH']['IN']['dataset']
            self.obc_in_path = self.config_data['PATH']['IN']['dataset']
            self.ncep_in_path = self.config_data['PATH']['IN']['dataset']
            self.coeff_in_path = self.config_data['PATH']['IN']['dataset']

            # ###################MID#######################
            # 中间文件的储存路径
            self.calibrate_mid_path = self.config_data['PATH']['MID']['dataset']
            self.incfg_mid_path = self.config_data['PATH']['MID']['dataset']
            self.ncep_mid_path = self.config_data['PATH']['MID']['dataset']
            self.granule_mid_path = self.config_data['PATH']['MID']['dataset']
            self.projection_mid_path = self.config_data['PATH']['MID']['dataset']

            # ###################OUT#######################
            # 输出的产品的存放路径
            self.daily_out_path = self.config_data['PATH']['OUT']['dataset']
            self.day_10_out_path = self.config_data['PATH']['OUT']['dataset']
            self.monthly_out_path = self.config_data['PATH']['OUT']['dataset']
            self.quarterly_out_path = self.config_data['PATH']['OUT']['dataset']
            self.yearly_out_path = self.config_data['PATH']['OUT']['dataset']
            self.log_out_path = self.config_data['PATH']['OUT']['dataset']
        except Exception as why:
            print why
            self.error = True
            print "Load config file error: {}".format(self.config_file)

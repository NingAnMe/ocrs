#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/7/25 10:24
@Author  : AnNing
"""

import os
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta

import numpy as np
import re
from PB.CSC.pb_csc_console import LogServer
from PB.pb_io import Config
from PB.pb_time import time_block, ymd2date
from app.bias import Bias
from app.config import InitApp
from app.plot import plot_time_series
from app.read_data import ReadCrossData

TIME_TEST = False  # 时间测试
RED = '#f63240'
BLUE = '#1c56fb'
GRAY = '#c0c0c0'
EDGE_GRAY = '#303030'


def main(sat_sensor, in_file):
    """
    对L3数据进行合成
    :param sat_sensor: 卫星+传感器
    :param in_file: yaml 文件
    :return:
    """
    # ######################## 初始化 ###########################
    # 获取程序所在位置，拼接配置文件
    app = InitApp(sat_sensor)
    if app.error:
        print "Load config file error."
        return

    gc = app.global_config
    # sc = app.sat_config
    yc = Config(in_file)
    log = LogServer(gc.path_out_log)

    # 加载全局配置信息

    # 加载程序配置信息

    # 加载卫星配置信息
    # ######################## 开始处理 ###########################
    print "-" * 100
    print "Start plot verify result."

    if not os.path.isfile(in_file):
        log.error("File is not exist: {}".format(in_file))
        return

    print "<<< {}".format(in_file)

    all_files = yc.path_ipath
    # 加载数据
    data_absolute = dict()
    data_relative = dict()
    date = dict()
    date_start = ymd2date(yc.info_ymd_s)
    date_end = ymd2date(yc.info_ymd_e)
    while date_start <= date_end:
        ymd_now = date_start.strftime('%Y%m%d')
        in_files = get_one_day_files(all_files=all_files, ymd=ymd_now, ext='.h5',
                                     pattern_ymd=r'.*_(\d{8})')
        cross_data = ReadCrossData()
        cross_data.read_cross_data(in_files=in_files)

        # 循环通道数据
        for channel in cross_data.data:
            if channel not in data_absolute:
                data_absolute[channel] = list()
            if channel not in data_relative:
                data_relative[channel] = list()
            if channel not in date:
                date[channel] = list()
            ref_s1 = cross_data.data[channel]['S1_FovRefMean']
            if len(ref_s1) == 0:
                print 'Dont have enough point to plot, is 0: {}'.format(channel)
                continue
            ref_s2 = cross_data.data[channel]['S2_FovRefMean']
            # 计算相对偏差和绝对偏差
            bias = Bias()
            absolute_bias = bias.absolute_deviation(ref_s1, ref_s2)
            relative_bias = bias.relative_deviation(ref_s1, ref_s2)
            data_absolute[channel].append(np.mean(absolute_bias))
            data_relative[channel].append(np.mean(relative_bias))
            date[channel].append(date_start)

        date_start = date_start + relativedelta(days=1)

    for channel in data_absolute:
        absolute_bias = data_absolute[channel]
        if len(absolute_bias) == 0:
            print 'Dont have enough point to plot, is 0: {}'.format(channel)
            continue
        relative_bias = data_relative[channel]
        date_channel = date[channel]
        # 绘制时间序列图
        title_series = '{} TIME SERIES {}'.format(sat_sensor, channel)
        y_label_series = 'Bias'
        picture_path = yc.path_opath
        picture_name_absolute = 'Time_Series_Absolute_Bias_{}.png'.format(channel)
        picture_name_relative = 'Time_Series_Relative_Bias_{}.png'.format(channel)
        picture_file_absolute = os.path.join(picture_path, picture_name_absolute)
        picture_file_relative = os.path.join(picture_path, picture_name_relative)
        plot_time_series(day_data_x=date_channel, day_data_y=absolute_bias,
                         out_file=picture_file_absolute,
                         title=title_series, y_label=y_label_series, ymd_start=yc.info_ymd_s,
                         ymd_end=yc.info_ymd_e, )
        plot_time_series(day_data_x=date_channel, day_data_y=relative_bias,
                         out_file=picture_file_relative,
                         title=title_series, y_label=y_label_series, ymd_start=yc.info_ymd_s,
                         ymd_end=yc.info_ymd_e, )

    print '-' * 100


def get_one_day_files(all_files, ymd, ext=None, pattern_ymd=None):
    """
    :param all_files: 文件列表
    :param ymd:
    :param ext: 后缀名, '.hdf5'
    :param pattern_ymd: 匹配时间的模式, 可以是 r".*(\d{8})_(\d{4})_"
    :return: list
    """
    files_found = []
    if pattern_ymd is not None:
        pattern = pattern_ymd
    else:
        pattern = r".*(\d{8})"

    for file_name in all_files:
        if ext is not None:
            if '.' not in ext:
                ext = '.' + ext
            if os.path.splitext(file_name)[1].lower() != ext.lower():
                continue
        re_result = re.match(pattern, file_name)
        if re_result is not None:
            time_file = ''.join(re_result.groups())
        else:
            continue
        if int(time_file) == int(ymd):
            files_found.append(os.path.join(file_name))
    return files_found


# ######################### 程序全局入口 ##############################
# if __name__ == "__main__":
#     # 获取程序参数接口
#     ARGS = sys.argv[1:]
#     HELP_INFO = \
#         u"""
#         [arg1]：sat+sensor
#         [arg2]：yaml file
#         [arg3]: is_time_series [bool]
#         [example]： python app.py arg1 arg2
#         """
#     if "-h" in ARGS:
#         print HELP_INFO
#         sys.exit(-1)
#
#     if len(ARGS) == 2:
#         SAT_SENSOR = ARGS[0]
#         FILE_PATH = ARGS[1]
#
#         with time_block("All", switch=TIME_TEST):
#             main(SAT_SENSOR, FILE_PATH)
#     else:
#         print HELP_INFO
#         sys.exit(-1)

######################### TEST ##############################
if __name__ == '__main__':
    yaml_file = r'E:\projects\oc_data\20130103154613_MERSI_MODIS.yaml'
    main('FY3B+MERSI', yaml_file)

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/7/24 10:17
@Author  : AnNing
"""
import os
import sys
import numpy as np
from PB.CSC.pb_csc_console import LogServer
from PB.pb_io import Config
from PB.pb_time import time_block
from app.bias import Bias
from app.config import InitApp
from app.plot import plot_map, plot_histogram, plot_scatter
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
    sc = app.sat_config
    yc = Config(in_file)
    log = LogServer(gc.path_out_log)

    # 加载全局配置信息
    sat_sensor1, sat_sensor2 = sat_sensor.split('_')
    sat1, sensor1 = sat_sensor1.split('+')
    sat2, sensor2 = sat_sensor2.split('+')
    # 加载卫星配置信息
    s_channel1 = sc.chan1
    s_channel2 = sc.chan2
    # 加载业务配置信息
    # ######################## 开始处理 ###########################
    print "-" * 100
    print "Start plot verify result."

    if not os.path.isfile(in_file):
        log.error("File is not exist: {}".format(in_file))
        return

    print "<<< {}".format(in_file)

    in_files = yc.path_ipath
    # 加载数据
    cross_data = ReadCrossData()
    cross_data.read_cross_data(in_files=in_files)

    # 循环通道数据
    for channel in cross_data.data:
        ref_s1 = cross_data.data[channel]['S1_FovRefMean']
        if len(ref_s1) == 0:
            print 'Dont have enough point to plot, is 0: {}'.format(channel)
            continue
        ref_s2 = cross_data.data[channel]['S2_FovRefMean']
        lat = cross_data.data[channel]['S1_Lat']
        lon = cross_data.data[channel]['S1_Lon']
        # 计算相对偏差和绝对偏差
        bias = Bias()
        absolute_bias = bias.absolute_deviation(ref_s1, ref_s2)
        relative_bias = bias.relative_deviation(ref_s1, ref_s2)

        # 绘制直方图
        channel1 = channel
        index_channel1 = s_channel1.index(channel1)
        channel2 = s_channel2[index_channel1]
        title_hist = '{}_{} {}_{} Histogram'.format(sat_sensor1, channel1, sat_sensor2,
                                                        channel2)
        x_label_hist_absolute = 'Dif  {}-{}'.format(sensor1, sensor2)
        x_label_hist_relative = 'Pdif  ({}/{})-1'.format(sensor1, sensor2)
        y_label_hist = 'Count'
        hist_label_absolute = 'Dif'
        hist_label_relative = 'Pdif'
        bins_count = 200
        picture_path = yc.path_opath
        picture_name_absolute = 'Histogram_Dif_{}_{}.png'.format(sat_sensor1, channel)
        picture_name_relative = 'Histogram_PDif_{}_{}.png'.format(sat_sensor1, channel)
        picture_file_absolute = os.path.join(picture_path, picture_name_absolute)
        picture_file_relative = os.path.join(picture_path, picture_name_relative)
        plot_histogram(data=absolute_bias, title=title_hist, x_label=x_label_hist_absolute,
                       y_label=y_label_hist, bins_count=bins_count, hist_label=hist_label_absolute,
                       out_file=picture_file_absolute,
                       ymd_start=yc.info_ymd_s, ymd_end=yc.info_ymd_e)
        plot_histogram(data=relative_bias, title=title_hist, x_label=x_label_hist_relative,
                       y_label=y_label_hist, bins_count=bins_count, hist_label=hist_label_relative,
                       out_file=picture_file_relative,
                       ymd_start=yc.info_ymd_s, ymd_end=yc.info_ymd_e)
        # 绘制偏差分布图
        title_scatter = '{} {}'.format(sat_sensor, channel)
        y_label_scatter = 'Bias'
        x_label_scatter = 'REF'
        fix_point = sc.plot_scatter_fix_ref
        fix_dif, fix_pdif = get_dif_pdif(ref_s1, ref_s2, fix_point)
        annotate_scatter = {'left_top': ['', 'Dif@{:.2f}={:.4f}'.format(fix_point, fix_dif),
                                         'PDif@{:.2f}={:.4f}'.format(fix_point, fix_pdif)]}
        picture_path = yc.path_opath
        picture_name_absolute = 'Scatter_Dif_{}_{}.png'.format(sat_sensor1, channel)
        picture_name_relative = 'Scatter_PDif_{}_{}.png'.format(sat_sensor1, channel)
        picture_file_absolute = os.path.join(picture_path, picture_name_absolute)
        picture_file_relative = os.path.join(picture_path, picture_name_relative)
        plot_scatter(data_x=ref_s1, data_y=absolute_bias, out_file=picture_file_absolute,
                     title=title_scatter, x_label=x_label_scatter, y_label=y_label_scatter,
                     ymd_start=yc.info_ymd_s, ymd_end=yc.info_ymd_e, annotate=annotate_scatter)
        plot_scatter(data_x=ref_s1, data_y=relative_bias, out_file=picture_file_relative,
                     title=title_scatter, x_label=x_label_scatter, y_label=y_label_scatter,
                     ymd_start=yc.info_ymd_s, ymd_end=yc.info_ymd_e, )
        # 绘制全球分布图
        title_map = '{} GLOBAL DISTRIBUTION {}'.format(sat_sensor, channel)
        picture_path = yc.path_opath
        picture_name = 'Map_Dif_{}_{}.png'.format(sat_sensor1, channel)
        picture_file_map = os.path.join(picture_path, picture_name)
        plot_map(lat=lat, lon=lon, data=absolute_bias, out_file=picture_file_map,
                 title=title_map)

    print '-' * 100


def get_dif_pdif(data1, data2, fix_point):
    """
    关于偏差的计算，请增加特定反射率处的偏差量计算，
    匹配后MERSI反射率为x，MODIS反射率为y, 拟合线yfit=ax+b
    特定反射率x_typical若取0.25
    Dif@0.25=x_typical-yfit(x_typical)
    PDif@0.25=x_typical/yfit(x_typical)-1
    :param data1:
    :param data2:
    :param fix_point:
    :return:
    """
    # ----- 计算回归直线特殊值的绝对偏差和相对偏差
    p1 = np.poly1d(np.polyfit(data1, data2, 1))
    ploy_fix_point = p1(fix_point)
    fix_dif = fix_point - ploy_fix_point
    fix_pdif = fix_point / ploy_fix_point - 1
    return fix_dif, fix_pdif


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
    yaml_file = r'D:\nsmc\occ_data\20130103154613_MERSI_MODIS.yaml'
    main('FY3B+MERSI_AQUA+MODIS', yaml_file)

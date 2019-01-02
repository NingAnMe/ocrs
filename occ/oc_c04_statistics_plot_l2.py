#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/12/24 10:17
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
from app.plot import plot_bias_map, plot_histogram, plot_regression
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
    sat_sensor1 = sat_sensor.split('_')[0]
    sat_sensor2 = sat_sensor.split('_')[1]
    sat1, sensor1 = sat_sensor1.split('+')
    sat2, sensor2 = sat_sensor2.split('+')
    # 加载卫星配置信息
    s_channel1 = sc.name
    s_channel2 = sc.name
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
    info = {}
    for channel in cross_data.data:
        point_count_min = 10

        if not isinstance(cross_data.data[channel], dict):
            continue

        mask_fine = cross_data.data[channel]['MaskFine']
        fine_idx = np.where(mask_fine > 0)
        fine_count = len(mask_fine[fine_idx])
        print '---INFO--- {} Points: {}'.format(channel, fine_count)
        if fine_count < point_count_min:
            print '***WARNING***Dont have enough point to plot: < {}'.format(point_count_min)
            continue

        ref_s1_all = cross_data.data[channel]['MERSI_FovMean']
        lat_s1_all = cross_data.data['MERSI_Lats']
        lon_s1_all = cross_data.data['MERSI_Lons']

        ref_s1 = ref_s1_all[fine_idx]
        lat = lat_s1_all[fine_idx]
        lon = lon_s1_all[fine_idx]

        ref_s2_all = cross_data.data[channel]['MODIS_FovMean']

        ref_s2 = ref_s2_all[fine_idx]

        # 过滤 3 倍std之外的点
        mean_ref_s1 = np.nanmean(ref_s1)
        std_ref_s1 = np.nanstd(ref_s1)
        min_ref_s1 = mean_ref_s1 - 3 * std_ref_s1
        max_ref_s1 = mean_ref_s1 + 3 * std_ref_s1
        idx = np.logical_and(ref_s1 >= min_ref_s1, ref_s1 <= max_ref_s1)
        ref_s1 = ref_s1[idx]
        ref_s2 = ref_s2[idx]
        lat = lat[idx]
        lon = lon[idx]

        # 计算相对偏差和绝对偏差
        bias = Bias()
        absolute_bias = bias.absolute_deviation(ref_s1, ref_s2)
        relative_bias = bias.relative_deviation(ref_s1, ref_s2)

        mean_absolute = np.nanmean(absolute_bias)
        std_absolute = np.nanstd(absolute_bias)
        amount_absolute = len(absolute_bias)
        median_absolute = np.nanmedian(absolute_bias)
        rms_absolute = rms(absolute_bias)

        mean_relative = np.nanmean(relative_bias)
        std_relative = np.nanstd(relative_bias)
        amount_relative = len(relative_bias)
        median_relative = np.nanmedian(relative_bias)
        rms_relative = rms(relative_bias)

        mean_ref_s1 = np.nanmean(ref_s1)
        std_ref_s1 = np.nanstd(ref_s1)
        amount_ref_s1 = len(ref_s1)
        median_ref_s1 = np.nanmedian(ref_s1)
        rms_ref_s1 = rms(ref_s1)

        mean_ref_s2 = np.nanmean(ref_s2)
        std_ref_s2 = np.nanstd(ref_s2)
        amount_ref_s2 = len(ref_s2)
        median_ref_s2 = np.nanmedian(ref_s2)
        rms_ref_s2 = rms(ref_s2)

        ################################### REF #######################################
        # 绘制REF直方图
        channel1 = channel
        index_channel1 = s_channel1.index(channel1)
        channel2 = s_channel2[index_channel1]
        title_hist_sat1 = '{}_{} Histogram'.format(sat_sensor1, channel1)
        title_hist_sat2 = '{}_{} Histogram'.format(sat_sensor2, channel2)
        x_label_hist_sat1 = 'REF  {}'.format(sat_sensor1)
        x_label_hist_sat2 = 'REF  {}'.format(sat_sensor2)
        y_label_hist = 'Count'
        bins_count = 200
        picture_path = yc.path_opath
        picture_name_sat1 = 'Histogram_REF_{}_{}.png'.format(sat_sensor1, channel1)
        picture_name_sat2 = 'Histogram_REF_{}_{}.png'.format(sat_sensor2, channel2)

        annotate_hist_sat1 = {'left_top': ['REF@Mean={:.4f}'.format(mean_ref_s1),
                                           'REF@Std={:.4f}'.format(std_ref_s1),
                                           'REF@Median={:.4f}'.format(median_ref_s1),
                                           'REF@RMS={:.4f}'.format(rms_ref_s1),
                                           'REF@Count={:4d}'.format(amount_ref_s1),
                                           ]}
        annotate_hist_sat2 = {'left_top': ['REF@Mean={:.4f}'.format(mean_ref_s2),
                                           'REF@Std={:.4f}'.format(std_ref_s2),
                                           'REF@Median={:.4f}'.format(median_ref_s2),
                                           'REF@RMS={:.4f}'.format(rms_ref_s2),
                                           'REF@Count={:4d}'.format(amount_ref_s2),
                                           ]}
        picture_file_sat1 = os.path.join(picture_path, picture_name_sat1)
        picture_file_sat2 = os.path.join(picture_path, picture_name_sat2)
        plot_histogram(data=ref_s1, title=title_hist_sat1, x_label=x_label_hist_sat1,
                       y_label=y_label_hist, bins_count=bins_count, out_file=picture_file_sat1,
                       ymd_start=yc.info_ymd_s, ymd_end=yc.info_ymd_e,
                       annotate=annotate_hist_sat1, )
        plot_histogram(data=ref_s2, title=title_hist_sat2, x_label=x_label_hist_sat2,
                       y_label=y_label_hist, bins_count=bins_count, out_file=picture_file_sat2,
                       ymd_start=yc.info_ymd_s, ymd_end=yc.info_ymd_e,
                       annotate=annotate_hist_sat2, )
        # 绘制REF回归图
        title_regression = '{}_{} {}_{} Diagonal Regression'.format(sat_sensor1, channel1,
                                                                    sat_sensor2,
                                                                    channel2)
        x_label_regression = 'REF {}'.format(sat_sensor1)
        y_label_regression = 'REF {}'.format(sat_sensor2)
        annotate_regression = {'left_top': ['MERSI@Mean={:.4f}'.format(mean_ref_s1),
                                            'MERSI@Std={:.4f}'.format(std_ref_s1),
                                            'MERSI@Median={:.4f}'.format(median_ref_s1),
                                            'MERSI@RMS={:.4f}'.format(rms_ref_s1),
                                            'MERSI@Count={:4d}'.format(amount_ref_s1),
                                            ]}
        picture_path = yc.path_opath
        picture_name_regression = 'Diagonal_Regression_{}_{}_{}_{}.png'.format(
            sat_sensor1, channel1, sat_sensor2, channel2)
        picture_file_regression = os.path.join(picture_path, picture_name_regression)
        plot_regression(data_x=ref_s1, data_y=ref_s2, out_file=picture_file_regression,
                        title=title_regression, x_label=x_label_regression,
                        y_label=y_label_regression,
                        ymd_start=yc.info_ymd_s, ymd_end=yc.info_ymd_e,
                        annotate=annotate_regression, plot_zero=False)

        ################################### REF 偏差 #######################################
        fix_point = sc.plot_scatter_fix_ref
        fix_dif, fix_pdif = get_dif_pdif(ref_s1, ref_s2, fix_point)
        annotate_absolute = {'left_top': ['Dif@{:.2f}={:.4f}'.format(fix_point, fix_dif),
                                          'Dif@Mean={:.4f}'.format(mean_absolute),
                                          'Dif@Std={:.4f}'.format(std_absolute),
                                          'Dif@Median={:.4f}'.format(median_absolute),
                                          'Dif@RMS={:.4f}'.format(rms_absolute),
                                          'Dif@Count={:4d}'.format(amount_absolute),
                                          ]}
        annotate_relative = {'left_top': ['PDif@{:.2f}={:.4f}'.format(fix_point, fix_pdif),
                                          'PDif@Mean={:.4f}'.format(mean_relative),
                                          'PDif@Std={:.4f}'.format(std_relative),
                                          'PDif@Median={:.4f}'.format(median_relative),
                                          'PDif@RMS={:.4f}'.format(rms_relative),
                                          'PDif@Count={:4d}'.format(amount_relative),
                                          ]}

        # 绘制偏差直方图
        channel1 = channel
        index_channel1 = s_channel1.index(channel1)
        channel2 = s_channel2[index_channel1]
        title_hist = '{}_{} {}_{} Histogram'.format(sat_sensor1, channel1, sat_sensor2,
                                                    channel2)
        x_label_hist_absolute = 'Dif  {}-{}'.format(sensor1, sensor2)
        x_label_hist_relative = 'PDif  ({}/{})-1'.format(sensor1, sensor2)
        y_label_hist = 'Count'
        bins_count = 200
        picture_path = yc.path_opath
        picture_name_absolute = 'Histogram_Dif_{}_{}_{}_{}.png'.format(sat_sensor1, channel1,
                                                                       sat_sensor2, channel2)
        picture_name_relative = 'Histogram_PDif_{}_{}_{}_{}.png'.format(sat_sensor1, channel1,
                                                                        sat_sensor2, channel2)
        annotate_hist_absolute = annotate_absolute
        annotate_hist_relative = annotate_relative
        picture_file_absolute = os.path.join(picture_path, picture_name_absolute)
        picture_file_relative = os.path.join(picture_path, picture_name_relative)
        plot_histogram(data=absolute_bias, title=title_hist, x_label=x_label_hist_absolute,
                       y_label=y_label_hist, bins_count=bins_count, out_file=picture_file_absolute,
                       ymd_start=yc.info_ymd_s, ymd_end=yc.info_ymd_e,
                       annotate=annotate_hist_absolute, )
        plot_histogram(data=relative_bias, title=title_hist, x_label=x_label_hist_relative,
                       y_label=y_label_hist, bins_count=bins_count, out_file=picture_file_relative,
                       ymd_start=yc.info_ymd_s, ymd_end=yc.info_ymd_e,
                       annotate=annotate_hist_relative, )
        # 绘制偏差散点图
        title_scatter = '{}_{} {}_{} Scattergram'.format(sat_sensor1, channel1, sat_sensor2,
                                                         channel2)
        y_label_scatter_absolute = 'Dif  {}-{}'.format(sensor1, sensor2)
        y_label_scatter_relative = 'PDif  ({}/{})-1'.format(sensor1, sensor2)
        x_label_scatter = 'REF {}'.format(sat_sensor1)
        annotate_scatter_absolute = annotate_absolute
        annotate_scatter_relative = annotate_relative
        picture_path = yc.path_opath
        picture_name_absolute = 'Scattergram_Dif_{}_{}_{}_{}.png'.format(sat_sensor1, channel1,
                                                                         sat_sensor2,
                                                                         channel2)
        picture_name_relative = 'Scattergram_PDif_{}_{}_{}_{}.png'.format(sat_sensor1, channel1,
                                                                          sat_sensor2,
                                                                          channel2)
        picture_file_absolute = os.path.join(picture_path, picture_name_absolute)
        picture_file_relative = os.path.join(picture_path, picture_name_relative)
        plot_regression(data_x=ref_s1, data_y=absolute_bias, out_file=picture_file_absolute,
                        title=title_scatter, x_label=x_label_scatter,
                        y_label=y_label_scatter_absolute,
                        ymd_start=yc.info_ymd_s, ymd_end=yc.info_ymd_e,
                        annotate=annotate_scatter_absolute, plot_slope=False, plot_zero=False)
        plot_regression(data_x=ref_s1, data_y=relative_bias, out_file=picture_file_relative,
                        title=title_scatter, x_label=x_label_scatter,
                        y_label=y_label_scatter_relative,
                        ymd_start=yc.info_ymd_s, ymd_end=yc.info_ymd_e,
                        annotate=annotate_scatter_relative, plot_slope=False, plot_zero=False)
        # 绘制偏差全球分布图
        title_map_absolute = '{}_{} {}_{} Global Distribution Dif {}-{}'.format(
            sat_sensor1, channel1, sat_sensor2, channel2, sensor1, sensor2)
        title_map_relative = '{}_{} {}_{} Global Distribution PDif ({}/{})-1'.format(
            sat_sensor1, channel1, sat_sensor2, channel2, sensor1, sensor2)

        picture_name_absolute = 'Map_Dif_{}_{}_{}_{}.png'.format(
            sat_sensor1, channel1, sat_sensor2, channel2)
        picture_name_relative = 'Map_PDif_{}_{}_{}_{}.png'.format(
            sat_sensor1, channel1, sat_sensor2, channel2)
        picture_path = yc.path_opath
        picture_file_map_absolute = os.path.join(picture_path, picture_name_absolute)
        picture_file_map_relative = os.path.join(picture_path, picture_name_relative)

        plot_bias_map(lat=lat, lon=lon, data=absolute_bias, out_file=picture_file_map_absolute,
                      title=title_map_absolute)
        plot_bias_map(lat=lat, lon=lon, data=relative_bias, out_file=picture_file_map_relative,
                      title=title_map_relative, vmin=-0.2, vmax=0.2)

    keys = info.keys()
    keys.sort()
    for channel in keys:
        print 'CHANNEL: {} POINT: {}'.format(channel, info[channel])
    print '-' * 100


def rms(x):
    """
    计算 数据的 RMS
    :param x:
    :return:
    """
    return np.sqrt(np.mean(x ** 2))


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


# ######################## 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    ARGS = sys.argv[1:]
    HELP_INFO = \
        u"""
        [arg1]：sat+sensor
        [arg2]：yaml file
        [arg3]: is_time_series [bool]
        [example]： python app.py arg1 arg2
        """
    if "-h" in ARGS:
        print HELP_INFO
        sys.exit(-1)

    if len(ARGS) == 2:
        SAT_SENSOR = ARGS[0]
        FILE_PATH = ARGS[1]

        with time_block("All", switch=TIME_TEST):
            main(SAT_SENSOR, FILE_PATH)
    else:
        print HELP_INFO
        sys.exit(-1)

# ######################### TEST ##############################
# if __name__ == '__main__':
#     yaml_file = r'20110101_20181231.yaml'
#     main('FY3B+MERSI_AQUA+MODIS', yaml_file)

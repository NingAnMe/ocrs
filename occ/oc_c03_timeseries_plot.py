#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/7/25 10:24
@Author  : AnNing
"""

import os
import re

import h5py
import numpy as np
import sys
from PB.CSC.pb_csc_console import LogServer
from PB.pb_io import Config, make_sure_path_exists
from PB.pb_time import ymd2date, time_block
from app.bias import Bias
from app.config import InitApp
from app.plot import plot_time_series
from app.read_data import ReadCrossData
from dateutil.relativedelta import relativedelta

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
    timseries_channels_config = sc.timeseries_channels
    # 加载业务配置信息
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

    result = dict()
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
            if channel not in result:
                result[channel] = dict()

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

            mean_absolute = np.nanmean(absolute_bias)
            std_absolute = np.nanstd(absolute_bias)
            amount_absolute = len(absolute_bias)
            median_absolute = np.nanmedian(absolute_bias)

            mean_relative = np.nanmean(relative_bias)
            std_relative = np.nanstd(relative_bias)
            amount_relative = len(relative_bias)
            median_relative = np.nanmedian(relative_bias)

            mean_ref_s1 = np.nanmean(ref_s1)
            std_ref_s1 = np.nanstd(ref_s1)
            amount_ref_s1 = len(ref_s1)
            median_ref_s1 = np.nanmedian(ref_s1)

            mean_ref_s2 = np.nanmean(ref_s2)
            std_ref_s2 = np.nanstd(ref_s2)
            amount_ref_s2 = len(ref_s2)
            median_ref_s2 = np.nanmedian(ref_s2)

            result_names = ['Dif_mean', 'Dif_std', 'Dif_median', 'Dif_count',
                            'PDif_mean', 'PDif_std', 'PDif_median', 'PDif_count',
                            'Ref_s1_mean', 'Ref_s1_std', 'Ref_s1_median', 'Ref_s1_count',
                            'Ref_s2_mean', 'Ref_s2_std', 'Ref_s2_median', 'Ref_s2_count',
                            'Date']
            datas = [mean_absolute, std_absolute, amount_absolute, median_absolute,
                     mean_relative, std_relative, amount_relative, median_relative,
                     mean_ref_s1, std_ref_s1, amount_ref_s1, median_ref_s1,
                     mean_ref_s2, std_ref_s2, amount_ref_s2, median_ref_s2,
                     ymd_now]
            for result_name, data in zip(result_names, datas):
                if result_name not in result[channel]:
                    result[channel][result_name] = list()
                else:
                    result[channel][result_name].append(data)

        date_start = date_start + relativedelta(days=1)

    for channel in data_absolute:
        absolute_bias = data_absolute[channel]

        plot_config = timseries_channels_config[channel]
        try:
            dif_y_range = plot_config['dif_y_range']
            pdif_y_range = plot_config['pdif_y_range']
        except:
            dif_y_range = None
            pdif_y_range = None

        if len(absolute_bias) == 0:
            print 'Dont have enough point to plot, is 0: {}'.format(channel)
            continue
        relative_bias = data_relative[channel]
        date_channel = date[channel]
        # 绘制时间序列图
        channel1 = channel
        index_channel1 = s_channel1.index(channel1)
        channel2 = s_channel2[index_channel1]
        title_series = '{}_{} {}_{} Time Series'.format(sat_sensor1, channel1, sat_sensor2,
                                                        channel2)
        y_label_series_absolute = 'Dif  {}-{}'.format(sensor1, sensor2)
        y_label_series_relative = 'PDif  ({}/{})-1'.format(sensor1, sensor2)
        picture_path = yc.path_opath
        picture_name_absolute = 'Time_Series_Dif_{}_{}_{}_{}.png'.format(sat_sensor1, channel1,
                                                                         sat_sensor2, channel2)
        picture_name_relative = 'Time_Series_PDif_{}_{}_{}_{}.png'.format(sat_sensor1, channel1,
                                                                          sat_sensor2, channel2)
        picture_file_absolute = os.path.join(picture_path, picture_name_absolute)
        picture_file_relative = os.path.join(picture_path, picture_name_relative)
        plot_time_series(day_data_x=date_channel, day_data_y=absolute_bias,
                         y_range=dif_y_range,
                         out_file=picture_file_absolute,
                         title=title_series, y_label=y_label_series_absolute,
                         ymd_start=yc.info_ymd_s, ymd_end=yc.info_ymd_e, )
        plot_time_series(day_data_x=date_channel, day_data_y=relative_bias,
                         y_range=pdif_y_range,
                         out_file=picture_file_relative,
                         title=title_series, y_label=y_label_series_relative,
                         ymd_start=yc.info_ymd_s, ymd_end=yc.info_ymd_e, )

    # 输出HDF5
    hdf5_name = '{}_{}_Dif_PDif.HDF'.format(sat_sensor1, sat_sensor2)
    out_path = yc.path_opath
    out_file_hdf5 = os.path.join(out_path, hdf5_name)
    make_sure_path_exists(out_path)
    write_hdf5(out_file_hdf5, result)

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


def write_hdf5(out_file, datas):
    """
    :param out_file: (str)
    :param datas: (dict)
    :return:
    """
    if not datas:
        return
    with h5py.File(out_file, 'w') as hdf5:
        for key in datas:
            if isinstance(datas[key], dict):
                group_name = key
                group_data = datas[key]
                if isinstance(group_data, dict):
                    for dataset_name in group_data:
                        data = group_data[dataset_name]
                        # 处理
                        if dataset_name != 'Date':
                            hdf5.create_dataset('/{}/{}'.format(group_name, dataset_name),
                                                dtype=np.float32, data=data)
                        else:
                            hdf5.create_dataset('/{}/{}'.format(group_name, dataset_name),
                                                data=data)
            else:
                dataset_name = key
                data = datas[dataset_name]
                # 处理
                hdf5.create_dataset(dataset_name, data=data)
    print '>>> {}'.format(out_file)


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
#     yaml_file = r'D:\nsmc\occ_data\20130103154613_MERSI_MODIS.yaml'
#     main('FY3B+MERSI_AQUA+MODIS', yaml_file)

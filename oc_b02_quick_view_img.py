# coding:utf-8
"""
绘制 HDF5.dataset 的快视图。支持真彩图和灰度图
~~~~~~~~~~~~~~~~~~~
creation time : 2018 5 5
author : anning
~~~~~~~~~~~~~~~~~~~
"""
import os
import sys

import h5py

from PB.CSC.pb_csc_console import LogServer

from PB.pb_io import is_none
from PB.pb_time import time_block

from app.config import InitApp
from app.plot import RGB, QuickView


TIME_TEST = False  # 时间测试


def main(sat_sensor, in_file):
    """
    绘制 HDF5.dataset 的快视图。支持真彩图和灰度图
    :param sat_sensor: 卫星+传感器
    :param in_file: HDF5 文件
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

    log = LogServer(gc.path_out_log)
    log = LogServer(gc.path_out_log)

    # 加载全局配置信息

    # 加载程序配置信息

    # 加载卫星配置信息
    dataset = sc.plt_quick_view_rgb_dataset
    rgb_suffix = sc.plt_quick_view_rgb_suffix
    colorbar_range = sc.plt_quick_view_img_colorbar_range
    log10_set = sc.plt_quick_view_img_log10_set
    log10_ticks = sc.plt_quick_view_img_log10_ticks
    log10_tick_labels = sc.plt_quick_view_img_log10_tick_labels

    # ######################## 开始处理 ###########################
    print '-' * 100
    print "Start plot quick view picture."
    if not os.path.isfile(in_file):
        log.error("File not exist: {}".format(in_file))
        return

    print "<<< {}".format(in_file)
    in_file_name = os.path.splitext(in_file)[0]

    # 绘制真彩图
    out_picture = "{}_{}.{}".format(in_file_name, rgb_suffix, "png")

    # 如果文件已经存在，跳过
    if not os.path.isfile(out_picture):
        r_set, g_set, b_set = dataset
        rgb = RGB(in_file, r_set, g_set, b_set, out_picture)
        rgb.plot()
        if not rgb.error:
            print ">>> {}".format(out_picture)
            print '-' * 100
        else:
            print "Error: Plot RGB error: {}".format(in_file)
    else:
        print "File is already exist, skip it: {}".format(out_picture)

    # 绘制热度图
    for legend in colorbar_range:
        dataset_name = legend[0]  # 数据集名称
        vmax = float(legend[1])  # color bar 范围 最大值
        vmin = float(legend[2])  # color bar 范围 最小值

        out_picture = "{}_{}.{}".format(in_file_name, dataset_name.replace("Aod", "AOD"), "png")
        # 如果文件已经存在，跳过
        # if os.path.isfile(out_picture):
        #     print "File is already exist, skip it: {}".format(out_picture)
        #     continue

        heat_map = {
            "vmin": vmin,
            "vmax": vmax,
            "cmap": "jet",
            "fill_value": -32767,
        }
        if dataset_name in log10_set:
            if dataset_name == "Ocean_TSM":
                log10_ticks.append(2.00)
                log10_tick_labels.append("100")
            if dataset_name == "Ocean_YS443":
                log10_ticks = log10_ticks[:-3]
                log10_tick_labels = log10_tick_labels[:-3]
            heat_map["colorbar_ticks"] = log10_ticks
            heat_map["colorbar_tick_label"] = log10_tick_labels

        lats, lons = get_lats_lons(in_file)
        lat_lon_text = _get_lat_lon_text(lats, lons)
        if is_none(lats, lons):
            lat_lon_line = None
        else:
            lat_lon_line = {
                "lats": lats,  # 经度数据集名称
                "lons": lons,  # 维度数据集名称
                "step": 5.0,  # 线密度
                "line_width": 0.01,
                "text": lat_lon_text,
            }

        quick_view = QuickView(in_file, dataset_name, out_picture, main_view=heat_map,
                               lat_lon_line=lat_lon_line)
        quick_view.plot()

        if not quick_view.error:
            print ">>> {}".format(quick_view.out_picture)
        else:
            print "Error: Plot heat view error: {}".format(in_file)
            print '-' * 100


def get_lats_lons(hdf5_file):
    """
    获取经纬度数据
    """
    lats = None
    lons = None

    try:
        with h5py.File(hdf5_file, 'r') as h5:
            lats = h5.get("Latitude")[:]
            lons = h5.get("Longitude")[:]

    except ValueError as why:
        print why

    return lats, lons


def _get_lat_lon_text(lats, lons):
    """
    获取在图片上添加的经纬度文字
    :return:
    """
    left_top = "{:5.2f}:{:5.2f}".format(lats[0][0], lons[0][0])
    left_bottom = "{:5.2f}:{:5.2f}".format(lats[-1][0], lons[-1][0])
    right_top = "{:5.2f}:{:5.2f}".format(lats[0][-1], lons[0][-1])
    right_bottom = "{:5.2f}:{:5.2f}".format(lats[-1][-1], lons[-1][-1])

    text = {
        "left_top": left_top,
        "left_bottom": left_bottom,
        "right_top": right_top,
        "right_bottom": right_bottom,
    }

    return text


######################### 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    ARGS = sys.argv[1:]
    HELP_INFO = \
        u"""
        [arg1]：sat+sensor
        [arg2]：hdf_file
        [example]： python app.py arg1 arg2
        """
    if "-h" in ARGS:
        print HELP_INFO
        sys.exit(-1)

    if len(ARGS) != 2:
        print HELP_INFO
        sys.exit(-1)
    else:
        SAT_SENSOR = ARGS[0]
        FILE_PATH = ARGS[1]

        with time_block("Calibrate time:", switch=TIME_TEST):
            main(SAT_SENSOR, FILE_PATH)

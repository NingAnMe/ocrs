# coding:utf-8
"""
水色产品的全球分布图绘制
~~~~~~~~~~~~~~~~~~~
creation time : 2018 5 16
author : anning
~~~~~~~~~~~~~~~~~~~
"""
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import re
import sys

from PB.pb_time import time_block
from PB.CSC.pb_csc_console import LogServer

from app.config import InitApp
from app.plot import PlotMapL3

TIME_TEST = False  # 时间测试


def main(sat_sensor, in_file):
    """
    绘制 L3 产品的全球投影图。
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

    # 加载全局配置信息

    # 加载程序配置信息

    # 加载卫星配置信息
    colorbar_range = sc.plt_combine_colorbar_range
    area_range = sc.plt_combine_area_range
    plot_global = sc.plt_combine_plot_global
    plot_china = sc.plt_combine_plot_china
    log10_ticks = sc.plt_combine_log10_ticks
    log10_tick_labels = sc.plt_combine_log10_tick_label
    log10_set = sc.plt_combine_log10_set
    # ######################## 开始处理 ###########################
    print "-" * 100
    print "Start plot combine map."

    if not os.path.isfile(in_file):
        log.error("File is not exist: {}".format(in_file))
        return

    print "<<< {}".format(in_file)

    sat, sensor = sat_sensor.split("+")
    for legend in colorbar_range:
        print "*" * 100
        dataset_name, name, vmax, vmin, colorbar_label = legend
        vmax = float(vmax)  # color bar 范围 最大值
        vmin = float(vmin)  # color bar 范围 最小值

        dir_path = os.path.dirname(in_file)
        ymd = _get_ymd(in_file)
        kind = _get_kind(in_file)

        ymd_date = datetime.strptime(ymd, "%Y%m%d")

        if dataset_name in log10_set:
            _ticks = log10_ticks
            _tick_labels = log10_tick_labels
            if dataset_name == "Ocean_TSM":
                _ticks.append(2.00)
                _tick_labels.append("100")
            if dataset_name == "Ocean_YS443":
                _ticks = _ticks[:-3]
                _tick_labels = _tick_labels[:-3]
        else:
            _ticks = None
            _tick_labels = None

        png = "{}_{}_{}_{}.png".format(sat_sensor, dataset_name.replace("Aod", "AOD"), ymd, kind)
        title = _get_title(sat, sensor, name, kind, ymd_date)

        plot_map = {
            "title": title,
            "legend": {"vmax": vmax, "vmin": vmin, "label": colorbar_label, "ticks": _ticks,
                       "tick_labels": _tick_labels},
            "area_range": area_range,
            "lat_lon_line": {"delat": 30, "delon": 30, },
        }

        # 画全球范围
        if plot_global.lower() == "on":
            pic_name = os.path.join(dir_path, "picture_global", png)
            # 如果输出文件已经存在，跳过
            if os.path.isfile(pic_name):
                print "File is already exist, skip it: {}".format(pic_name)
            else:
                if dataset_name in log10_set:
                    plot_map["log10"] = True

                with time_block("Draw combine time:", switch=TIME_TEST):
                    plot_map_global = PlotMapL3(in_file, dataset_name, pic_name, map_=plot_map)
                    plot_map_global.draw_combine()

                if not plot_map_global.error:
                    print ">>> {}".format(plot_map_global.out_file)
                else:
                    print "Error: Plot global picture error: {}".format(in_file)

        # 单画中国区域
        if plot_china.lower() == "on":
            pic_name = os.path.join(dir_path, "picture_china", png)
            # 如果输出文件已经存在，跳过
            if os.path.isfile(pic_name):
                print "File is already exist, skip it: {}".format(pic_name)
            else:
                area_range_china = {
                    "lat_s": "56",
                    "lat_n": "2",
                    "lon_w": "65",
                    "lon_e": "150",
                }
                lat_lon_line = {"delat": 10, "delon": 10, }
                plot_map["area_range"] = area_range_china
                plot_map["lat_lon_line"] = lat_lon_line

                with time_block("Draw combine time:", switch=TIME_TEST):
                    plot_map_china = PlotMapL3(in_file, dataset_name, pic_name, map_=plot_map)
                    plot_map_china.draw_combine()

                if not plot_map_china.error:
                    print ">>> {}".format(plot_map_china.out_file)
                else:
                    print "Error: Plot china picture error: {}".format(in_file)

    print '-' * 100


def _get_title(sat, sensor, name, kind, ymd_date):
    """
    根据不同时间的产品获取title
    :param kind:
    :return:
    """
    if kind == "AOAD":
        ymd_dash = ymd_date.strftime("%Y-%m-%d")
        title = "{}/{} daily Level-3 product {} {}".format(
            sat, sensor, name, ymd_dash)
    elif kind == "AOAM":
        ymd_start_dash = ymd_date.strftime("%Y-%m-%d")
        ymd_end_date = ymd_date + relativedelta(months=1) - relativedelta(days=1)
        ymd_end_dash = ymd_end_date.strftime("%Y-%m-%d")
        title = "{}/{} monthly Level-3 product {} {} to {}".format(
            sat, sensor, name, ymd_start_dash, ymd_end_dash)
    else:
        title = "{}/{} Level-3 product {}".format(
            sat, sensor, name)
    return title


def _get_ymd(l3_file):
    """
    从输入的L3文件中获取 ymd
    :param l3_file:
    :return:
    """
    if not isinstance(l3_file, str):
        return
    m = re.match(r".*_(\d{8})_", l3_file)

    if m is None:
        return
    else:
        return m.groups()[0]


def _get_kind(l3_file_name):
    """
    获取 L3 产品中的合成种类名称
    """
    m = re.match(r".*_\d{4,8}_(\w{4})_", l3_file_name)
    try:
        kind = m.groups()[0]
    except Exception as why:
        print why
        kind = "AOAD"
    return kind


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

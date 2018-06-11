# coding:utf-8
"""
~~~~~~~~~~~~~~~~~~~
creation time : 2018 1 24
author : anning
~~~~~~~~~~~~~~~~~~~
"""
import re
import os
import sys
import h5py

from PB.CSC.pb_csc_console import LogServer
from PB import pb_io, pb_time, pb_calculate
from PB.pb_time import time_block
from DV.dv_img import dv_rgb

from app.config import InitApp
from app.calibrate import Calibrate

TIME_TEST = True  # 时间测试


def main(sat_sensor, in_file):
    """
    使用矫正系数对 MERSI L1 的产品进行定标预处理
    :param sat_sensor: (str) 卫星对
    :param in_file: (str) 输入文件
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

    # 全局配置接口
    l1_path = gc.path_in_l1
    obc_path = gc.path_in_obc
    coeff_path = gc.path_in_coeff
    out_path = gc.path_mid_calibrate
    # 程序配置接口

    # 卫星配置接口
    launch_date = sc.calibrate_launch_date
    probe_count = sc.calibrate_probe_count
    probe = sc.calibrate_probe
    slide_step = sc.calibrate_slide_step
    plot = sc.calibrate_plot

    ######################### 开始处理 ###########################
    print '-' * 100
    print 'Start calibration'

    # 获取 M1000 文件和对应 OBC 文件
    l1_1000m = in_file
    obc_1000m = _get_obc_file(l1_1000m, l1_path, obc_path)
    if not os.path.isfile(l1_1000m):
        log.error("File is not exist: {}".format(l1_1000m))
        return
    elif not os.path.isfile(obc_1000m):
        log.error("File is not exist: {}".format(obc_1000m))
        return
    else:
        print "<<< {}".format(l1_1000m)
        print "<<< {}".format(obc_1000m)

    ymd = _get_ymd(l1_1000m)

    # 获取 coefficient 水色波段系统定标系数， 2013年以前和2013年以后不同
    coeff_file = os.path.join(coeff_path, '{}.txt'.format(ymd[0:4]))
    if not os.path.isfile(coeff_file):
        log.error("File is not exist: {}".format(coeff_file))
        return
    else:
        print "<<< {}".format(coeff_file)

    # 获取输出文件
    out_path = pb_io.path_replace_ymd(out_path, ymd)
    _name = os.path.basename(l1_1000m)
    out_file = os.path.join(out_path, _name)

    # 如果输出文件已经存在，跳过预处理
    if os.path.isfile(out_file):
        print "File is already exist, skip it: {}".format(out_file)
        return

    # 初始化一个预处理实例
    calibrate = Calibrate(l1_1000m=l1_1000m, obc_1000m=obc_1000m, coeff_file=coeff_file,
                          out_file=out_file, launch_date=launch_date)

    # 对 OBC 文件进行 SV 提取
    calibrate.obc_sv_extract_fy3b(probe=probe, probe_count=probe_count,
                                  slide_step=slide_step)

    # 重新定标 L1 数据
    calibrate.calibrate()

    # 将新数据写入 HDF5 文件
    calibrate.write()

    if not calibrate.error:
        print ">>> {}".format(calibrate.out_file)
    else:
        print "Error: Calibrate error".format(in_file)

    # 对原数据和处理后的数据各出一张真彩图
    if plot == "on":
        if not calibrate.error:
            picture_suffix = "650_565_490"
            file_name = os.path.splitext(out_file)[0]
            out_pic_old = "{}_{}_old.{}".format(file_name, picture_suffix, "png")
            out_pic_new = "{}_{}_new.{}".format(file_name, picture_suffix, "png")
            # 如果输出文件已经存在，跳过
            if os.path.isfile(out_pic_old):
                print "File is already exist, skip it: {}".format(out_pic_old)
                return
            else:
                _plot_rgb(in_file, out_pic_old)
            if os.path.isfile(out_pic_new):
                print "File is already exist, skip it: {}".format(out_pic_new)
                return
            else:
                _plot_rgb(out_file, out_pic_new)

    print '-' * 100


def _get_ymd(l1_file):
    """
    从输入的L1文件中获取 ymd
    :param l1_file:
    :return:
    """
    if not isinstance(l1_file, str):
        return
    m = re.match(r".*_(\d{8})_", l1_file)

    if m is None:
        return
    else:
        return m.groups()[0]


def _plot_rgb(l1_file, out_file):
    """
    对原数据和处理后的数据各出一张真彩图
    """
    try:
        with h5py.File(l1_file) as h5:
            r = h5.get("EV_1KM_RefSB")[5]  # 第 11 通道 650
            g = h5.get("EV_1KM_RefSB")[4]  # 第 10 通道 565
            b = h5.get("EV_1KM_RefSB")[2]  # 第 8 通道 490
        dv_rgb(r, g, b, out_file)
        print ">>> {}".format(out_file)
    except Exception as why:
        print why
        print "Error: plot RGB error".format(l1_file)
        return


def _get_obc_file(m1000_file, m1000_path, obc_path):
    """
    通过 1KM 文件路径生成 OBC 文件的路径
    :param m1000_file:
    :param m1000_path:
    :param obc_path:
    :return:
    """
    m1000_path = m1000_path.replace("%YYYY/%YYYY%MM%DD", '')
    obc_path = obc_path.replace("%YYYY/%YYYY%MM%DD", '')

    obc_file = m1000_file.replace(m1000_path, obc_path)
    obc_file = obc_file.replace("_1000M", "_OBCXX")

    return obc_file


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

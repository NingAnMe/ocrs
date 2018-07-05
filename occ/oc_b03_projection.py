# coding:utf-8
"""
将水色产品在全球进行投影，获取投影后的位置信息和数据的位置查找表信息
存储在 HDF5 文件
~~~~~~~~~~~~~~~~~~~
creation time : 2018 5 9
author : anning
~~~~~~~~~~~~~~~~~~~
"""
import re
import os
import sys

from PB import pb_io, pb_time
from PB.pb_time import time_block
from PB.pb_space import deg2meter
from PB.CSC.pb_csc_console import LogServer

from app.config import InitApp
from app.projection import Projection

TIME_TEST = False  # 时间测试


def main(sat_sensor, in_file):
    """
    对L2数据进行投影，记录投影后的位置信息和数据信息
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
    out_path = gc.path_mid_projection

    # 加载程序配置信息

    # 加载卫星配置信息
    res = sc.project_res
    half_res = deg2meter(res) / 2.
    cmd = sc.project_cmd % (half_res, half_res)
    row = sc.project_row
    col = sc.project_col
    mesh_size = sc.project_mesh_size

    # ######################## 开始处理 ###########################
    print "-" * 100
    print "Start projection."

    if not os.path.isfile(in_file):
        log.error("File is not exist: {}".format(in_file))
        return

    print "<<< {}".format(in_file)

    with time_block("One project time:", switch=TIME_TEST):
        # 生成输出文件的文件名
        ymd = _get_ymd(in_file)
        hm = _get_hm(in_file)
        sat, sensor = sat_sensor.split('+')
        out_name = "{}_{}_ORBT_L2_OCC_MLT_NUL_{}_{}_{}.HDF".format(sat, sensor, ymd, hm,
                                                                   mesh_size.upper())
        out_path = pb_io.path_replace_ymd(out_path, ymd)
        out_file = os.path.join(out_path, out_name)

        # 如果输出文件已经存在，跳过
        if os.path.isfile(out_file):
            print "File is already exist, skip it: {}".format(out_file)
            return

        # 开始创建投影查找表
        projection = Projection(cmd=cmd, row=row, col=col, res=res)
        projection.project(in_file, out_file)

        if not projection.error:
            print ">>> {}".format(out_file)
        else:
            print "Error: Projection error: {}".format(in_file)

        print "-" * 100


def _get_ymd(l2_file):
    """
    从输入L2文件中获取 ymd
    :param l2_file:
    :return:
    """
    if not isinstance(l2_file, str):
        return
    m = re.match(r".*_(\d{8})_", l2_file)

    if m is None:
        return
    else:
        return m.groups()[0]


def _get_hm(l2_file):
    """
    从L2文件中获取 hm
    :param l2_file:
    :return:
    """
    if not isinstance(l2_file, str):
        return
    m = re.match(r".*_(\d{4})_", l2_file)

    if m is None:
        return
    else:
        return m.groups()[0]


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

        with time_block("Projection time:", switch=TIME_TEST):
            main(SAT_SENSOR, FILE_PATH)

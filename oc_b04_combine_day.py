# coding:utf-8
"""
水色产品的日合成程序：将一天多个时次的数据合成为一天的数据
~~~~~~~~~~~~~~~~~~~
creation time : 2018 5 11
author : anning
~~~~~~~~~~~~~~~~~~~
"""
import os
import sys

from PB.pb_time import time_block
from PB.pb_space import deg2meter
from PB.CSC.pb_csc_console import LogServer

from app.config import InitApp
from app.combine import CombineL2

TIME_TEST = False  # 时间测试


def main(sat_sensor, in_file):
    """
    对L2数据进行合成
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

    log = LogServer(gc.path_out_log)

    # 加载全局配置信息

    # 加载程序配置信息

    # 加载卫星配置信息
    res = sc.project_res
    half_res = deg2meter(res)/ 2.
    cmd = sc.project_cmd % (half_res, half_res)
    row = sc.project_row
    col = sc.project_col

    # ######################## 开始处理 ###########################
    print "-" * 100
    print "Start combine."

    if not os.path.isfile(in_file):
        log.error("File is not exist: {}".format(in_file))
        return

    print "<<< {}".format(in_file)

    combine = CombineL2()  # 初始化一个投影实例
    combine.load_cmd_info(cmd=cmd, res=res, row=row, col=col)
    combine.load_yaml(in_file)  # 加载 yaml 文件

    with time_block("One combine time:", switch=TIME_TEST):
        combine.combine()

    with time_block("One write time:", switch=TIME_TEST):
        combine.write()

    if not combine.error:
        print ">>> {}".format(combine.ofile)
    else:
        print "Error: Combine day error: {}".format(in_file)

    print "-" * 100


######################### 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    ARGS = sys.argv[1:]
    HELP_INFO = \
        u"""
        [arg1]：sat+sensor
        [arg2]：yaml file
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

# coding:utf-8
"""
将多个日合成后的 HDF5 文件合成为一个 HDF5 文件
~~~~~~~~~~~~~~~~~~~
creation time : 2018 5 21
author : anning
~~~~~~~~~~~~~~~~~~~
"""
import os
import sys

from PB.pb_time import time_block
from PB.CSC.pb_csc_console import LogServer

from app.config import InitApp
from app.combine import CombineL3

TIME_TEST = True  # 时间测试


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

    log = LogServer(gc.path_out_log)

    # 加载全局配置信息

    # 加载程序配置信息

    # 加载卫星配置信息

    # ######################## 开始处理 ###########################
    print "-" * 100
    print "Start plot combine map."

    if not os.path.isfile(in_file):
        log.error("File is not exist: {}".format(in_file))
        return

    print "<<< {}".format(in_file)

    combine = CombineL3()  # 初始化一个合成实例
    combine.load_yaml(in_file)  # 加载 yaml 文件

    with time_block("One combine time:", switch=TIME_TEST):
        combine.combine()
    with time_block("One write time:", switch=TIME_TEST):
        combine.write()

    if not combine.error:
        print ">>> {}".format(combine.ofile)
    else:
        print "Error: Combine days error: {}".format(in_file)

    print '-' * 100


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

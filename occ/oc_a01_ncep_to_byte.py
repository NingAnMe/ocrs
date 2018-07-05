# coding:utf-8
import os
import re
import sys

from PB.CSC.pb_csc_console import LogServer
from PB import pb_io, pb_time
from PB.pb_time import time_block

from app.config import InitApp
from app.ncep_to_byte import Ncep2Byte

TIME_TEST = False  # 时间测试


def main(sat_sensor, in_file):
    """
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
    out_path = gc.path_mid_ncep
    # 程序配置接口

    # 卫星配置接口
    suffix = sc.ncep2byte_filename_suffix
    ncep_table = sc.ncep2byte_ncep_table
    ######################### 开始处理 ###########################
    print "-" * 100
    print "Start ncep to byte."
    if not os.path.isfile(in_file):
        log.error("File is not exist: {}".format(in_file))
        return

    print "<<< {}".format(in_file)

    ymdhm = _get_ymdhm(in_file)
    out_file = _get_out_file(in_file, out_path, suffix)

    # 判断是否使用新命令进行处理
    if int(ymdhm) >= 201501221200:
        new = True
    else:
        new = False

    if pb_io.is_none(in_file, out_file, ncep_table):
        log.error("Error: {}".format(in_file))
        return

    ncep2byte = Ncep2Byte(in_file, out_file, new=new, ncep_table=ncep_table)
    ncep2byte.ncep2byte()

    if not ncep2byte.error:
        print ">>> {}".format(ncep2byte.out_file)
    else:
        log.error("Error: {}".format(in_file))

    print "-" * 100


def _get_ymdhm(ncep_file):
    """
    从 ncep 文件的文件名中获取 ymdhm
    :return:
    """
    try:
        result = re.match(r".*_(\d{8})_(\d{2})_(\d{2})", ncep_file)
        ymdhm = result.groups()[0] + result.groups()[1] + result.groups()[2]
        return ymdhm
    except Exception as why:
        print why
        return


def _get_out_file(in_file, out_path, suffix):
    """
    通过输入的文件名和输出路径获取输出文件的完整路径
    :return:
    """
    try:
        if isinstance(in_file, str):
            ymd = pb_time.get_ymd(in_file)
            out_path = pb_io.path_replace_ymd(out_path, ymd)
            _name = os.path.basename(in_file)
            name = _name.replace("_c", "") + suffix
            out_file = os.path.join(out_path, name)
            if not os.path.isdir(os.path.dirname(out_file)):
                try:
                    os.makedirs(os.path.dirname(out_file))
                except OSError as why:
                    print why
            return out_file
    except Exception as why:
        print why
        return


######################### 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    ARGS = sys.argv[1:]
    HELP_INFO = \
        u"""
        [arg1]：sat+sensor
        [arg2]：ncep_file
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

        with time_block("Ncep to byte time:", switch=TIME_TEST):
            main(SAT_SENSOR, FILE_PATH)

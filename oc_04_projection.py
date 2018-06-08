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
from PB.CSC.pb_csc_console import LogServer
from PB.pb_io import Config

from app.config import GlobalConfig
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
    main_path = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(main_path, "cfg")
    global_config_file = os.path.join(config_path, "global.cfg")
    yaml_config_file = os.path.join(config_path, "ncep_to_byte.yaml")
    sat_config_file = os.path.join(config_path, "{}.yaml".format(sat_sensor))

    gc = GlobalConfig(global_config_file)
    pc = PROJConfig(yaml_config_file)
    sc = SatConfig(sat_config_file)

    if pc.error or gc.error or sc.error:
        print "Load config error"
        return

    log = LogServer(gc.log_out_path)

    # 加载全局配置信息
    out_path = gc.projection_mid_path

    # 加载程序配置信息

    # 加载卫星配置信息
    cmd = sc.cmd
    row = sc.row
    col = sc.col
    res = sc.res
    mesh_size = sc.mesh_size

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
        out_name = "{}_{}_ORBT_L2_ASO_MLT_NUL_{}_{}_{}.HDF".format(sat, sensor, ymd, hm,
                                                                   mesh_size.upper())
        out_path = pb_io.path_replace_ymd(out_path, ymd)
        out_file = os.path.join(out_path, out_name)

        # 如果输出文件已经存在，跳过
        if os.path.isfile(out_file):
            print "File is already exist, skip it: {}".format(out_file)
            return

        projection = Projection(cmd=cmd, row=row, col=col, res=res)
        # 开始创建投影查找表
        projection.project(in_file, out_file)
        if not projection.error:
            print ">>> {}".format(out_file)

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


class PROJConfig(Config):
    """
    加载程序的配置文件
    """
    def __init__(self, config_file):
        """
        初始化
        """
        Config.__init__(self, config_file)

        self.load_yaml_file()

        # 添加需要的配置信息
        try:
            pass
        except Exception as why:
            print why
            self.error = True
            print "Load config file error: {}".format(self.config_file)


class SatConfig(Config):
    """
    加载卫星的配置文件
    """
    def __init__(self, config_file):
        """
        初始化
        """
        Config.__init__(self, config_file)

        self.load_yaml_file()

        # 添加需要的配置信息
        try:
            self.cmd = self.config_data['project']['cmd']
            self.row = self.config_data['project']['row']
            self.col = self.config_data['project']['col']
            self.res = self.config_data['project']['res']
            self.mesh_size = self.config_data['project']['mesh_size']
        except Exception as why:
            print why
            self.error = True
            print "Load config file error: {}".format(self.config_file)


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

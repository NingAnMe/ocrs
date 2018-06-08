# coding:utf-8
"""
水色产品的全球分布图绘制
~~~~~~~~~~~~~~~~~~~
creation time : 2018 5 16
author : anning
~~~~~~~~~~~~~~~~~~~
"""
import os
import re
import sys

from PB import pb_io, pb_time
from PB.pb_time import time_block
from PB.CSC.pb_csc_console import LogServer
from PB.pb_io import Config

from app.config import GlobalConfig
from app.plot import PlotMapL3

TIME_TEST = True  # 时间测试


def main(sat_sensor, in_file):
    """
    绘制 L3 产品的全球投影图。
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

    # 加载程序配置信息

    # 加载卫星配置信息
    colorbar_range = sc.colorbar_range
    area_range = sc.area_range

    # ######################## 开始处理 ###########################
    print '-' * 100

    if os.path.isfile(in_file):
        print "Start draw combine picture: {}".format(in_file)
    else:
        log.error("File is not exist: {}".format(in_file))
        return

    for legend in colorbar_range:
        dataset_name = legend[0]  # 数据集名称
        vmax = float(legend[1])  # color bar 范围 最大值
        vmin = float(legend[2])  # color bar 范围 最小值
        dir_path = os.path.dirname(in_file)
        ymd = _get_ymd(in_file)
        kind = _get_kind(in_file)
        pic_name = os.path.join(dir_path, "pictures/{}_{}_{}_{}.png".format(
            sat_sensor, dataset_name, ymd, kind))

        # 如果输出文件已经存在，跳过
        # if os.path.isfile(pic_name):
        #     print "File is already exist, skip it: {}".format(pic_name)
        #     continue
        plot_map = {
            "title": "{}  {}".format(dataset_name, ymd),
            "legend": {"vmax": vmax, "vmin": vmin},
            "area_range": area_range
        }

        with time_block("Draw combine time:", switch=TIME_TEST):
            PlotMapL3(in_file, dataset_name, pic_name, plot_map=plot_map)

    print '-' * 100


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
            self.colorbar_range = self.config_data["plt_combine"]["colorbar_range"]
            self.area_range = self.config_data["plt_combine"]["area_range"]
            for k, v in self.area_range.items():
                self.area_range[k] = float(v)
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

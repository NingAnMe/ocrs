# coding:utf-8
import os
import re
import sys

from PB.CSC.pb_csc_console import LogServer
from PB import pb_io, pb_time
from PB.pb_time import time_block
from PB.pb_io import Config

from app.config import GlobalConfig
from app.ncep_to_byte import Ncep2Byte

TIME_TEST = True  # 时间测试


def main(sat_sensor, in_file):
    """
    :param sat_sensor: (str) 卫星对
    :param in_file: (str) 输入文件
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

    # 全局配置接口
    out_path = gc.ncep_mid_path
    # 程序配置接口

    # 卫星配置接口
    suffix = sc.filename_suffix
    ncep_table = sc.ncep_table
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
            # 生成的文件增加的后缀名
            self.filename_suffix = self.config_data["ncep2byte"]['filename_suffix']
            # 需要处理的 ncep 类型
            self.ncep_table = self.config_data["ncep2byte"]['ncep_table']
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

# coding:utf-8
import os
import re
import sys
from configobj import ConfigObj

from PB.CSC.pb_csc_console import LogServer
from PB import pb_io, pb_time
from PB.pb_time import time_block

TIME_TEST = True  # 时间测试


def run(in_file):
    ######################### 初始化 ###########################
    # 加载程序配置文件
    proj_cfg_file = os.path.join(main_path, "global.yaml")
    proj_cfg = pb_io.load_yaml_config(proj_cfg_file)
    if proj_cfg is None:
        log.error("File is not exist: {}".format(proj_cfg_file))
        return
    else:
        # 加载配置信息
        try:
            WGRIB1 = proj_cfg['ncep2byte']['wgrib1']
            WGRIB2 = proj_cfg['ncep2byte']['wgrib2']
            FILENAME_SUFFIX = proj_cfg['ncep2byte']['filename_suffix']
            if pb_io.is_none(WGRIB1, WGRIB2, FILENAME_SUFFIX):
                log.error("Yaml args is not completion. : {}".format(proj_cfg_file))
                return
        except ValueError:
            log.error("Load yaml config file error, please check it. : {}".format(proj_cfg_file))
            return

    ######################### 开始处理 ###########################
    if not os.path.isfile(in_file):
        log.error("File is not exist: {}".format(in_file))
        return
    else:
        print "-" * 100
        print "Start ncep to byte."
        out_path = OUT_PATH

        ncep2byte = Ncep2Byte(in_file, out_path, wgrib1=WGRIB1, wgrib2=WGRIB2, suffix=FILENAME_SUFFIX)
        ncep2byte.ncep2byte()

        print "-" * 100


class Ncep2Byte(object):
    def __init__(self, in_file, out_path, wgrib1=None, wgrib2=None, suffix=None):
        self.error = False

        self.in_file = in_file
        self.out_path = out_path

        self.ncep_table = None

        self.wgrib1 = wgrib1
        self.wgrib2 = wgrib2
        self.suffix = suffix

        self.cmd1 = None
        self.cmd2 = None

        self.out_file = None

    def _get_ncep_table(self):
        """
        获取需要处理的 ncep 类型
        :return:
        """
        if self.error:
            return
        self.ncep_table = [
            "PRES:sfc", "PWAT:atmos col", "UGRD:10 m above gnd", "VGRD:10 m above gnd", "TOZNE:atmos",
            "TMP:1000 mb", "TMP:925 mb", "TMP:850 mb",
            "TMP:700 mb", "TMP:500 mb", "TMP:400 mb", "TMP:300 mb", "TMP:250 mb", "TMP:200 mb",
            "TMP:150 mb", "TMP:100 mb", "TMP:70 mb", "TMP:50 mb", "TMP:30 mb",
            "TMP:20 mb", "TMP:10 mb", "RH:1000 mb", "RH:925 mb", "RH:850 mb",
            "RH:700 mb", "RH:500 mb", "RH:400 mb", "RH:300 mb", "LAND:sfc", "TMP:sfc", "ICEC:sfc"]

    def _get_cmd(self, ncep_type):
        """
        获取两个命令版本
        :param ncep_type:
        :return:
        """
        if self.error:
            return
        try:
            self.cmd1 = self.wgrib1 % (self.in_file, ncep_type, self.in_file, self.out_file)
            self.cmd2 = self.wgrib2 % (self.in_file, ncep_type, self.in_file, self.out_file)
        except Exception as why:
            print why
            self.error = True
            return

    def _get_hm(self):
        result = re.match(r".*_(\d{2})_(\d{2})", self.in_file)
        self.hm = result.groups()[0] + result.groups()[1]

    def _get_out_file(self):
        """
        通过输入的文件名和输出路径获取输出文件的完整路径
        :return:
        """
        if isinstance(self.in_file, str):
            self.ymd = pb_time.get_ymd(self.in_file)
            out_path = pb_io.path_replace_ymd(self.out_path, self.ymd)
            _name = os.path.basename(self.in_file)
            name = _name.replace("_c", "") + self.suffix
            self.out_file = os.path.join(out_path, name)
            if not os.path.isdir(os.path.dirname(self.out_file)):
                try:
                    os.makedirs(os.path.dirname(self.out_file))
                except OSError as why:
                    print why
        else:
            self.error = False
            return

    def _remove_file(self):
        """
        如果文件存在，删除原来的文件
        :return:
        """
        if os.path.isfile(self.out_file):
            os.remove(self.out_file)

    def ncep2byte(self):
        if self.error:
            return
        self._get_ncep_table()
        self._get_out_file()
        self._remove_file()  # 如果已经存在文件，删除文件后重新处理
        self._get_hm()
        ymdhm_int = int(self.ymd + self.hm)
        for ncep_type in self.ncep_table:
            self._get_cmd(ncep_type)
            # 201501221200 是两个命令的时间节点
            if ymdhm_int < 201501221200:
                os.system(self.cmd1)
            else:
                os.system(self.cmd2)
        print self.out_file


######################### 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    args = sys.argv[1:]
    help_info = \
        u"""
        [参数1]： NCEP文件
        [样例]： python 程序 NCEP文件
        """
    if "-h" in args:
        print help_info
        sys.exit(-1)

    # 获取程序所在位置，拼接配置文件
    main_path, main_file = os.path.split(os.path.realpath(__file__))
    project_path = main_path
    config_file = os.path.join(project_path, "global.cfg")

    # 配置不存在预警
    if not os.path.isfile(config_file):
        print (u"配置文件不存在 %s" % config_file)
        sys.exit(-1)

    # 载入配置文件
    inCfg = ConfigObj(config_file)
    LOG_PATH = inCfg["PATH"]["OUT"]["log"]
    log = LogServer(LOG_PATH)

    # 开启进程池
    # thread_number = inCfg["CROND"]["threads"]
    # thread_number = 1
    # pool = Pool(processes=int(thread_number))

    if not len(args) == 1:
        print help_info
    else:
        FILE_PATH = args[0]

        OUT_PATH = inCfg["PATH"]["MID"]["ncep"]  # 文件输出路径
        SAT = inCfg["PATH"]["sat"]
        SENSOR = inCfg["PATH"]["sensor"]
        SAT_SENSOR = "{}+{}".format(SAT, SENSOR)

        with time_block("Ncep to byte time:", switch=TIME_TEST):
            run(FILE_PATH)

        # pool.apply_async(run, (sat_sensor, file_path))
        # pool.close()
        # pool.join()

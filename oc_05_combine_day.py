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

import h5py
import yaml
import numpy as np
from configobj import ConfigObj

from DP.dp_prj_new import prj_core
from PB import pb_io
from PB.pb_time import time_block
from PB.pb_space import deg2meter
from PB.CSC.pb_csc_console import LogServer


TIME_TEST = True  # 时间测试


def run(sat_sensor, yaml_file):
    ######################### 初始化 ###########################
    # 加载程序配置文件
    proj_cfg_file = os.path.join(MAIN_PATH, "global.yaml")
    proj_cfg = pb_io.load_yaml_config(proj_cfg_file)
    if proj_cfg is None:
        LOG.error("File is not exist: {}".format(proj_cfg_file))
        return
    else:
        # 加载配置信息
        try:
            RES = proj_cfg['project'][sat_sensor]['res']
            half_res = deg2meter(RES) / 2.
            CMD = proj_cfg['project'][sat_sensor]['cmd'] % (half_res, half_res)
            ROW = proj_cfg['project'][sat_sensor]['row']
            COL = proj_cfg['project'][sat_sensor]['col']
            MESH_SIZE = proj_cfg['project'][sat_sensor]['mesh_zise']
            if pb_io.is_none(CMD, ROW, COL, RES, MESH_SIZE):
                LOG.error("Yaml args is not completion. : {}".format(proj_cfg_file))
                return
        except Exception as why:
            print why
            LOG.error("Load yaml config file error, please check it. : {}".format(proj_cfg_file))
            return

    ######################### 开始处理 ###########################
    # 判断 yaml 文件是否存在
    if not os.path.isfile(yaml_file):
        LOG.error("File is not exist: {}".format(yaml_file))
        return
    else:
        combine = Combine()  # 初始化一个投影实例
        combine.load_cmd_info(cmd=CMD, res=RES, row=ROW, col=COL)
        combine.load_yaml(yaml_file)  # 加载 yaml 文件

        with time_block("One combine time:", switch=TIME_TEST):
            combine.combine()

        with time_block("One write time:", switch=TIME_TEST):
            combine.write()


class Combine(object):

    def __init__(self):
        self.error = False

        self.res = None
        self.cmd = None
        self.col = None
        self.row = None

        self.yaml_file = None

        self.ifile = None
        self.pfile = None
        self.ofile = None

        self.in_data = {}
        self.attrs = {}
        self.out_data = {}

        self.lons = None
        self.lats = None

        self.lookup_table = None

        self.ii = None
        self.jj = None

        self.lut_ii = None
        self.lut_jj = None
        self.data_ii = None
        self.data_jj = None

    def load_cmd_info(self, cmd=None, res=None, row=None, col=None):
        """
        获取拼接投影命令的参数信息
        :param cmd: 
        :param res: 
        :param row: 
        :param col: 
        :return: 
        """
        if self.error:
            return
        self.cmd = cmd
        self.res = res
        self.row = row
        self.col = col

    def load_yaml(self, yaml_file):
        """
        读取 yaml 格式配置文件
        """
        if self.error:
            return
        if not os.path.isfile(yaml_file):
            print 'Not Found %s' % yaml_file
            self.error = True
            return

        self.yaml_file = yaml_file
        try:
            with open(yaml_file, 'r') as stream:
                cfg = yaml.load(stream)

            self.ifile = cfg['PATH']['ipath']
            self.pfile = cfg['PATH']['ppath']
            self.ofile = cfg['PATH']['opath']

        except Exception as why:
            print why
            LOG.error("Load yaml file error, please check it. : {}".format(yaml_file))
            self.error = True

    def load_proj_data(self, hdf5_file):
        if self.error:
            return
        # 加载数据
        if os.path.isfile(hdf5_file):
            try:
                with h5py.File(hdf5_file, 'r') as h5:
                    self.lut_ii = h5.get("lut_ii")[:]
                    self.lut_jj = h5.get("lut_jj")[:]
                    self.data_ii = h5.get("data_ii")[:]
                    self.data_jj = h5.get("data_jj")[:]

            except Exception as why:
                print why
                print "Can't open file: {}".format(hdf5_file)
                self.error = True
                return
        else:
            print "File does not exist: {}".format(hdf5_file)
            self.error = True
            return
        return

    def combine(self):
        if self.error:
            return

        # 如果输出文件已经存在，跳过
        elif os.path.isfile(self.ofile):
            LOG.error("File is already exist, skip it: {}".format(self.ofile))
            return
        # 合成日数据
        elif pb_io.is_none(self.ifile, self.pfile, self.ofile):
            self.error = True
            LOG.error("Is None: ifile or pfile or ofile: {}".format(self.yaml_file))
            return
        elif len(self.ifile) < 1:
            self.error = True
            LOG.error("File count lower than 1: {}".format(self.yaml_file))

        fill_value = -32767
        for file_idx, in_file in enumerate(self.ifile):
            proj_file = self.pfile[file_idx]
            if os.path.isfile(in_file) and os.path.isfile(proj_file):
                print "Start combining file:"
                print in_file, "\n", proj_file
            else:
                print "File is not exist: {} OR {}".format(in_file, proj_file)
                continue

            # 加载 proj 数据
            self.load_proj_data(proj_file)
            # 日合成
            with time_block("One combine time:", switch=TIME_TEST):
                try:
                    with h5py.File(in_file, 'r') as h5:
                        for k in h5.keys():
                            if k == "Longitude" or k == "Latitude":
                                continue
                            elif k not in self.out_data.keys():
                                if k == "Ocean_Flag":
                                    self.out_data[k] = np.full((self.row, self.col), fill_value, dtype='i4')
                                else:
                                    self.out_data[k] = np.full((self.row, self.col), fill_value, dtype='i2')
                            # 合并一个数据
                            proj_data = h5.get(k)[:]
                            self.out_data[k][self.lut_ii, self.lut_jj] = proj_data[self.data_ii, self.data_jj]

                            # 记录属性信息
                            if k not in self.attrs.keys():
                                self.attrs[k] = pb_io.attrs2dict(h5.get(k).attrs)
                    print '-' * 100

                except Exception as why:
                    print why
                    print "Can't combine file, some error exist: {}".format(in_file)

        with time_block("Grid to lons and lats time:", switch=TIME_TEST):
            if "Longitude" not in self.out_data.keys():
                lookup_table = prj_core(self.cmd, self.res, unit="deg", row=self.row, col=self.col)
                lookup_table.grid_lonslats()
                self.out_data["Longitude"] = lookup_table.lons
                self.out_data["Latitude"] = lookup_table.lats

        # 输出数据集有效数据的数量
        keys = self.out_data.keys()
        keys.sort()
        for k in keys:
            if self.out_data[k] is None:
                print k
                continue
            idx = np.where(self.out_data[k] > 0)
            print "{:30} : {}".format(k, len(idx[0]))

    def write(self):
        if self.error:
            return
        pb_io.make_sure_path_exists(os.path.dirname(self.ofile))
        # 写入 HDF5 文件
        with h5py.File(self.ofile, 'w') as h5:
            for k in self.out_data.keys():
                # 创建数据集
                if k == "Longitude" or k == "Latitude":
                    h5.create_dataset(k, dtype='f4',
                                      data=self.out_data[k],
                                      compression='gzip', compression_opts=5,
                                      shuffle=True)
                elif k == "Ocean_Flag":
                    h5.create_dataset(k, dtype='i4',
                                      data=self.out_data[k],
                                      compression='gzip', compression_opts=5,
                                      shuffle=True)
                else:
                    h5.create_dataset(k, dtype='i2',
                                      data=self.out_data[k],
                                      compression='gzip', compression_opts=5,
                                      shuffle=True)

                # 复制属性
                if k == "Longitude" or k == "Latitude":
                    continue
                attrs = self.attrs[k]
                for key, value in attrs.items():
                    h5[k].attrs[key] = value
        print "Output file: {}".format(self.ofile)


######################### 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    ARGS = sys.argv[1:]
    HELP_INFO = \
        u"""
        [arg1]：yaml_file
        [example]： python app.py arg1
        """
    if "-h" in ARGS:
        print HELP_INFO
        sys.exit(-1)

    # 获取程序所在位置，拼接配置文件
    MAIN_PATH = os.path.dirname(os.path.realpath(__file__))
    CONFIG_FILE = os.path.join(MAIN_PATH, "global.cfg")

    # 配置不存在预警
    if not os.path.isfile(CONFIG_FILE):
        print "File is not exist: {}".format(CONFIG_FILE)
        sys.exit(-1)

    # 载入配置文件
    IN_CFG = ConfigObj(CONFIG_FILE)
    LOG_PATH = IN_CFG["PATH"]["OUT"]["log"]
    LOG = LogServer(LOG_PATH)

    # 开启进程池
    # thread_number = IN_CFG["CROND"]["threads"]
    # thread_number = 1
    # pool = Pool(processes=int(thread_number))

    if not len(ARGS) == 1:
        print HELP_INFO
    else:
        FILE_PATH = ARGS[0]
        SAT = IN_CFG["PATH"]["sat"]
        SENSOR = IN_CFG["PATH"]["sensor"]
        SAT_SENSOR = "{}+{}".format(SAT, SENSOR)

        with time_block("All combine time:", switch=TIME_TEST):
            run(SAT_SENSOR, FILE_PATH)
        # pool.apply_async(run, (sat_sensor, file_path))
        # pool.close()
        # pool.join()

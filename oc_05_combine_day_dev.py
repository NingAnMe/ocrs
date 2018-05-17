# coding:utf-8

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


def run(pair, colloc_file):
    ######################### 初始化 ###########################
    # 加载程序配置文件
    proj_cfg_file = os.path.join(main_path, "global.yaml")
    proj_cfg = pb_io.load_yaml_config(proj_cfg_file)
    if proj_cfg is None:
        log.error("File is not exist: {}".format(proj_cfg_file))
        return

    ######################### 开始处理 ###########################
    # 判断 colloc 文件是否存在
    if not os.path.isfile(colloc_file):
        log.error("File is not exist: {}".format(colloc_file))
        return
    else:
        with time_block("All combine time:", switch=TIME_TEST):
            combine = Combine()  # 初始化一个投影实例
            combine.load_colloc(colloc_file)  # 加载 colloc 文件

            sat, sensor = pair.split("+")
            if sat != combine.sat or sensor != combine.sensor:
                return

            with time_block("One combine time:", switch=TIME_TEST):
                combine.combine()
            with time_block("One write time:", switch=TIME_TEST):
                combine.write()


class Combine(object):

    def __init__(self):
        self.error = False

        self.sat = None
        self.sensor = None
        self.ymd = None

        self.res = None
        self.cmd = None
        self.col = None
        self.row = None

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

    def load_colloc(self, colloc_file):
        """
        读取 yaml 格式配置文件
        """
        if self.error:
            return
        if not os.path.isfile(colloc_file):
            print 'Not Found %s' % colloc_file
            self.error = True
            return

        try:
            with open(colloc_file, 'r') as stream:
                cfg = yaml.load(stream)

            self.sat = cfg['INFO']['sat']
            self.sensor = cfg['INFO']['sensor']
            self.ymd = cfg['INFO']['ymd']

            self.ifile = cfg['PATH']['ipath']
            self.pfile = cfg['PATH']['ppath']
            self.ofile = cfg['PATH']['opath']

            self.res = cfg['PROJ']['res']

            half_res = deg2meter(self.res) / 2.
            self.cmd = cfg['PROJ']['cmd'] % (half_res, half_res)

            self.col = cfg['PROJ']['col']
            self.row = cfg['PROJ']['row']
        except Exception as why:
            print why
            log.error("Load colloc file error, please check it. : {}".format(colloc_file))
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
        # 合成日数据
        if self.ifile is None or self.ofile is None:
            self.error = True
            print "Is None: ifile or pfile: {}".format(self.ymd)
            return
        elif len(self.ifile) < 2:
            self.error = True
            print "File count lower than 2: {}".format(self.ymd)
        fillvalue = -32767.
        for file_idx, in_file in enumerate(self.ifile):
            proj_file = self.pfile[file_idx]
            if os.path.isfile(in_file) and os.path.isfile(proj_file):
                print "Start combining file:"
                print in_file, "\n", proj_file
            else:
                print "File is not exist:"
                print in_file, "\n", proj_file
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
                                self.out_data[k] = np.full((self.row, self.col), fillvalue, dtype='f4')

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
        for k, v in self.out_data.items():
            idx = np.where(v > 0)
            print len(idx[0]), k

    def write(self):
        if self.error:
            return
        pb_io.make_sure_path_exists(os.path.dirname(self.ofile))
        # 写入 HDF5 文件
        with h5py.File(self.ofile, 'w') as h5:
            for k in self.out_data.keys():
                # 创建数据集
                h5.create_dataset(k, dtype='f4',
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
    args = sys.argv[1:]
    help_info = \
        u"""
        [参数1]：SAT+SENSOR
        [参数2]：colloc 文件
        [样例]： python ocrs_combine.py FY3B+MERSI 20171012.colloc
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

    if not len(args) == 2:
        print help_info
    else:
        sat_sensor = args[0]
        file_path = args[1]

        with time_block("All combine time:", switch=TIME_TEST):
            run(sat_sensor, file_path)
        # pool.apply_async(run, (sat_sensor, file_path))
        # pool.close()
        # pool.join()

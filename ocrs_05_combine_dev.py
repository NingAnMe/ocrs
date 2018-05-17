# coding:utf-8

import os
import sys
import calendar
from datetime import datetime
from multiprocessing import Pool, Manager

import numpy as np
import h5py
import yaml
from matplotlib.ticker import MultipleLocator

from configobj import ConfigObj
from dateutil.relativedelta import relativedelta
from numpy.lib.polynomial import polyfit
from numpy.ma.core import std, mean
from numpy.ma.extras import corrcoef

from PB.CSC.pb_csc_console import LogServer
from DP.dp_prj_new import prj_core
from DV import dv_map
from PB import pb_time, pb_io
from PB.pb_space import deg2meter
from ocrs_io import loadYamlCfg
from publicmodels.pm_time import time_block


def run(pair, colloc_file):
    # 加载程序配置文件
    proj_cfg_file = os.path.join(main_path, "global.yaml")
    proj_cfg = loadYamlCfg(proj_cfg_file)
    if proj_cfg is None:
        log.error("File is not exist: {}".format(proj_cfg_file))
        return

    # 判断 colloc 文件是否存在
    if not os.path.isfile(colloc_file):
        log.error("File is not exist: {}".format(colloc_file))
        return

    else:
        with time_block("all combine"):
            combine = Combine()  # 初始化一个投影实例
            combine.load_colloc(colloc_file)  # 加载 colloc 文件
            with time_block("combine"):
                combine.combine()
            if combine.error:
                return
            with time_block("write"):
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
        # # 开发测试使用的 self.ifile,业务要注释掉下面这个语句
        # self.ifile = [
        #     "/storage-space/disk3/Granule/out_del_cloudmask/2017/201701/20170101/20170101_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20170101_{:0>4}_1000M.HDF".format(
        #         x, x) for x in xrange(0, 2500)]
        # self.pfile = [
        #     "/storage-space/disk3/Granule/out_del_cloudmask/2017/201701/20170101/20170101_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20170101_{:0>4}_1000M_PROJ.HDF".format(
        #         x, x) for x in xrange(0, 2500)]
        # self.ifile = [
        #     "/storage-space/disk3/Granule/out_del_cloudmask/2017/201710/20171012/20171012_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20171012_{:0>4}_1000M.HDF".format(
        #         x, x) for x in xrange(0, 2500)]
        # self.pfile = [
        #     "/storage-space/disk3/Granule/out_del_cloudmask/2017/201710/20171012/20171012_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20171012_{:0>4}_1000M_PROJ.HDF".format(
        #         x, x) for x in xrange(0, 2500)]
        # self.ifile = [
        #     "/storage-space/disk3/Granule/out_del_cloudmask/2013/201301/20130101/20130101_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20130101_{:0>4}_1000M.HDF".format(
        #         x, x) for x in xrange(0, 2500)]
        # self.pfile = [
        #     "/storage-space/disk3/Granule/out_del_cloudmask/2013/201301/20130101/20130101_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20130101_{:0>4}_1000M_PROJ.HDF".format(
        #         x, x) for x in xrange(0, 2500)]
        # self.ofile = "/storage-space/disk3/Granule/out_del_cloudmask/2013/201301/20130101/20130101_0000_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20130101_0000_1000M_COMBINE.HDF"
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
            with time_block("one combine time:"):
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
                                self.attrs[k] = attrs2dict(h5.get(k).attrs)
                    print '-' * 100

                except Exception as why:
                    print why
                    print "Can't combine file, some error exist: {}".format(in_file)

        with time_block("grid to lons and lats"):
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


def attrs2dict(attrs):
    """
    将一个 <class 'h5py._hl.attrs.AttributeManager'> 转换为字典
    :param attrs:
    :return:
    """
    attrs_dict = {}
    for key, value in attrs.items():
        attrs_dict[key] = value
    return attrs_dict


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
    LOG_PATH = inCfg["PATH"]["OUT"]["LOG"]
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
        with time_block("combine time:"):
            run(sat_sensor, file_path)
        # pool.apply_async(run, (sat_sensor, file_path))
        # pool.close()
        # pool.join()

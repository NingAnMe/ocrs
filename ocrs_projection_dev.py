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


def run(config_file):
    if not os.path.isfile(config_file):
        print 'Not Found %s' % config_file
        return

    with open(config_file, 'r') as stream:
        cfg = yaml.load(stream)
        ifile = cfg['PATH']['ipath']
        ofile = cfg['PATH']['ppath']

        res = cfg['PROJ']['res']

        half_res = deg2meter(res) / 2.
        cmd = cfg['PROJ']['cmd'] % (half_res, half_res)

        col = cfg['PROJ']['col']
        row = cfg['PROJ']['row']

    lookup_table = prj_core(cmd, res, unit="deg", row=row, col=col)
    ifile = ["/storage-space/disk3/Granule/out_del_cloudmask/2017/201701/20170101/20170101_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20170101_{:0>4}_1000M.HDF".format(x, x) for x in xrange(0, 2500)]
    ofile = [
        "/storage-space/disk3/Granule/out_del_cloudmask/2017/201701/20170101/20170101_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20170101_{:0>4}_1000M_PROJ.HDF".format(x, x) for x in xrange(0, 2500)]
    ifile = [
        "/storage-space/disk3/Granule/out_del_cloudmask/2017/201710/20171012/20171012_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20171012_{:0>4}_1000M.HDF".format(
            x, x) for x in xrange(0, 2500)]
    ofile = [
        "/storage-space/disk3/Granule/out_del_cloudmask/2017/201710/20171012/20171012_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20171012_{:0>4}_1000M_PROJ.HDF".format(
            x, x) for x in xrange(0, 2500)]
    for idx_file, in_file in enumerate(ifile):
        if not os.path.isfile(in_file):
            print in_file
            continue
        else:
            print in_file
        with time_block("one projection"):
            out_file = ofile[idx_file]
            projection = Projection()
            projection.load_yaml_config(config_file)
            projection.project(in_file, out_file, lookup_table)

        # with time_block("draw one projection picture"):
        #     dataset_name = "Ocean_Aod_550"
        #     projection.draw(in_file, out_file, dataset_name)


class Projection(object):

    def __init__(self):
        self.error = False

        self.sat = None
        self.sensor = None
        self.ymd = None

        self.cmd = None
        self.col = None
        self.row = None
        self.res = None

        self.in_data = {}
        self.attrs = {}
        self.out_data = {}

        self.lons = None
        self.lats = None

        self.lookup_table = None

        self.ii = None
        self.jj = None
        self.index = None
        self.index_ii = None
        self.index_jj = None
        self.data_ii = None
        self.data_jj = None

    def load_yaml_config(self, in_proj_cfg):
        """
        读取 yaml 格式配置文件
        """
        if not os.path.isfile(in_proj_cfg):
            print 'Not Found %s' % in_proj_cfg
            self.error = True
            return

        with open(in_proj_cfg, 'r') as stream:
            cfg = yaml.load(stream)

        self.sat = cfg['INFO']['sat']
        self.sensor = cfg['INFO']['sensor']
        self.ymd = cfg['INFO']['ymd']

        self.res = cfg['PROJ']['res']

        half_res = deg2meter(self.res) / 2.
        self.cmd = cfg['PROJ']['cmd'] % (half_res, half_res)

        self.col = cfg['PROJ']['col']
        self.row = cfg['PROJ']['row']

    def _load_lons_lats(self, in_file):
        # 加载数据
        if os.path.isfile(in_file):
            try:
                with h5py.File(in_file, 'r') as h5:
                    self.lons = h5.get("Longitude")[:]
                    self.lats = h5.get("Latitude")[:]
            except Exception as why:
                print why
                print "Can't open file: {}".format(in_file)
                self.error = True
                return
        else:
            print "File does not exist: {}".format(in_file)
            self.error = True
            return

    def _create_lut(self, lookup_table):
        # 创建查找表
        # with time_block("prj core"):
        #     self.lookup_table = prj_core(self.cmd, self.res, unit="deg", row=self.row, col=self.col)
        lookup_table.create_lut(self.lons, self.lats)
        self.ii = lookup_table.lut_i
        self.jj = lookup_table.lut_j

    def _get_index(self):
        # 获取数据的索引信息
        self.index = np.logical_and(self.ii >= 0, self.jj >= 0)
        self.index = np.where(self.index)  # 全球投影的行列索引信息
        self.index_ii = self.index[0].T
        self.index_jj = self.index[1].T
        self.data_ii = self.ii[self.index]  # 数据行索引信息
        self.data_jj = self.jj[self.index]  # 数据列索引信息

    def _write(self, out_file):
        pb_io.make_sure_path_exists(os.path.dirname(out_file))
        # 写入 HDF5 文件
        with h5py.File(out_file, 'w') as h5:
            # 创建数据集
            h5.create_dataset("data_ii", dtype='i2',
                              data=self.data_ii,
                              compression='gzip', compression_opts=1,
                              shuffle=True)
            h5.create_dataset("data_jj", dtype='i2',
                              data=self.data_jj,
                              compression='gzip', compression_opts=1,
                              shuffle=True)
            h5.create_dataset("index_ii", dtype='i2',
                              data=self.index_ii,
                              compression='gzip', compression_opts=1,
                              shuffle=True)
            h5.create_dataset("index_jj", dtype='i2',
                              data=self.index_jj,
                              compression='gzip', compression_opts=1,
                              shuffle=True)
        print out_file

    def project(self, in_file, out_file, lookup_table):
        with time_block("load lons and lats"):
            self._load_lons_lats(in_file)
        with time_block("create lut"):
            self._create_lut(lookup_table)
        with time_block("get index"):
            self._get_index()
        with time_block("write data"):
            self._write(out_file)

    def write(self, out_file):
        # 写入 HDF5 文件
        with h5py.File(out_file, 'w') as h5:
            for k in self.out_data.keys():
                # 创建数据集
                h5.create_dataset(k, dtype='f4',
                                  data=self.out_data[k],
                                  compression='gzip', compression_opts=1,
                                  shuffle=True)
                # 复制属性
                attrs = self.attrs[k]
                for key, value in attrs.items():
                    h5[k].attrs[key] = value

    def draw(self, in_file, proj_file, dataset_name):
        # 加载 Proj 数据
        if os.path.isfile(proj_file):
            try:
                with h5py.File(proj_file, 'r') as h5:
                    index_ii = h5.get("index_ii")[:]
                    index_jj = h5.get("index_jj")[:]
                    data_ii = h5.get("data_ii")[:]
                    data_jj = h5.get("data_jj")[:]
            except Exception as why:
                print why
                print "Can't open file: {}".format(proj_file)
                return
        else:
            print "File does not exist: {}".format(proj_file)
            return
        with time_block("draw load"):
            # 加载投影后的数据
            if os.path.isfile(in_file):
                try:
                    with h5py.File(in_file, 'r') as h5:
                        proj_value = h5.get(dataset_name)[:][data_ii, data_jj]
                except Exception as why:
                    print why
                    print "Can't open file: {}".format(in_file)
                    return
            else:
                print "File does not exist: {}".format(in_file)
                return
        out_png_path = os.path.dirname(in_file)
        out_png = os.path.join(out_png_path, '{}.png'.format(dataset_name))

        p = dv_map.dv_map()
        p.title = "{}    {}".format(dataset_name, self.ymd)

        # 增加省边界
        #       p1.show_china_province = True
        p.delat = 30
        p.delon = 30
        p.show_line_of_latlon = False
        #         p.colormap = 'gist_rainbow'
        #         p.colormap = 'viridis'
        #         p.colormap = 'brg'

        # 创建查找表
        lookup_table = prj_core(self.cmd, self.res, unit="deg", row=self.row, col=self.col)
        lookup_table.grid_lonslats()
        lons = lookup_table.lons
        lats = lookup_table.lats

        # 创建完整的数据投影
        fillValue = -999
        value = np.full((self.row, self.col), fillValue, dtype='f4')
        value[index_ii, index_jj] = proj_value
        value = np.ma.masked_less_equal(value, 0)  # 过滤 <=0 的数据

        p.easyplot(lats, lons, value, ptype=None, vmin=0, vmax=32767, markersize=0.1, marker='o')
        pb_io.make_sure_path_exists(os.path.dirname(out_png))
        p.savefig(out_png, dpi=300)
        print out_png


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


if __name__ == "__main__":
    # 获取程序参数接口
    args = sys.argv[1:]
    help_info = \
        u"""
        [参数1]：配置文件
        [样例]： python ocrs_projection.py 20171012.colloc
        """
    if "-h" in args:
        print help_info
        sys.exit(-1)

    if len(args) == 1:
        with time_block("all"):
            run(args[0])
    else:
        print help_info

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
        ofile = cfg['PATH']['opath']
    ifile = ["/storage-space/disk3/Granule/out_del_cloudmask/2017/201701/20170101/20170101_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20170101_{:0>4}_1000M.HDF".format(x, x) for x in xrange(0, 9999)]
    ofile = [
        "/storage-space/disk3/Granule/out_del_cloudmask/2017/201701/20170101/20170101_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20170101_{:0>4}_1000M_PROJ.HDF".format(x, x) for x in xrange(0, 9999)]
    for idx_file, in_file in enumerate(ifile):
        if not os.path.isfile(in_file):
            print in_file
            continue
        else:
            print in_file
        with time_block("one projection"):
            projection = Projection(in_file)  # 初始化一个投影实例
            projection.load_yaml_config(config_file)  # 加载配置文件
            if projection.error:
                continue
            with time_block("load data"):
                projection.load_data()
            if projection.error:
                continue
            with time_block("create lut"):
                projection.create_lut()  # 创建投影查找表
            with time_block("project"):
                projection.project()
            with time_block("write"):
                projection.write(ofile[idx_file])
            # with time_block("draw"):
            #     projection.draw()


class Projection(object):

    def __init__(self, in_file):
        self.error = False

        self.file = in_file

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

        self.lookup_table = None

        self.ii = None
        self.jj = None

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

    def load_data(self):
        # 加载数据
        if os.path.isfile(self.file):
            try:
                with h5py.File(self.file, 'r') as h5:
                    for k in h5.keys():
                        self.in_data[k] = h5.get(k)[:]
                        self.attrs[k] = attrs2dict(h5.get(k).attrs)
            except Exception as why:
                print why
                print "Can't open file: {}".format(self.file)
                self.error = True
                return
        else:
            print "File does not exist: {}".format(self.file)
            self.error = True
            return

    def create_lut(self):
        # 创建查找表
        self.lookup_table = prj_core(self.cmd, self.res, unit="deg", row=self.row, col=self.col)
        lon = self.in_data.get("Longitude")
        lat = self.in_data.get("Latitude")
        self.lookup_table.create_lut(lon, lat)

    def project(self):
        # 进行投影
        ii = self.lookup_table.lut_i
        jj = self.lookup_table.lut_j

        # idx = np.where(ii >= 0)
        idx = np.logical_and(ii >= 0, jj >= 0)
        for k in self.in_data.keys():
            if k == "Longitude" or k == "Latitude":
                continue
            else:
                if k in self.out_data.keys():
                    self.out_data[k][idx] = self.in_data[k][ii[idx], jj[idx]]
                else:
                    fillValue = -999
                    self.out_data[k] = np.full((self.row, self.col), fillValue, dtype='f4')
                    self.out_data[k][idx] = self.in_data[k][ii[idx], jj[idx]]

        # if "Longitude" not in self.out_data.keys():
        #     self.lookup_table.grid_lonslats()
        #     self.out_data["Longitude"] = self.lookup_table.lons
        #     self.out_data["Latitude"] = self.lookup_table.lats

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

    def draw(self):
        dataset_name = "Ocean_Aod_550"
        # out_png_path = os.path.dirname(self.ofile)
        out_png = os.path.join("", 'test.png')

        p = dv_map.dv_map()
        # p.title = u'%s_%s %s_%s IR%d tbb (calType %d)' % (sat1, sat2, date, hm, i, calType)
        p.title = "{}    {}".format(dataset_name, self.ymd)

        # 增加省边界
        #       p1.show_china_province = True
        p.delat = 30
        p.delon = 30
        p.show_line_of_latlon = False
        #         p.colormap = 'gist_rainbow'
        #         p.colormap = 'viridis'
        #         p.colormap = 'brg'
        lat = self.out_data["Latitude"]
        lon = self.out_data["Longitude"]
        for k, v in self.out_data.items():
            idx = np.where(v > 0)
            print len(idx[0]), k
        value = self.out_data[dataset_name]
        value = np.ma.masked_less_equal(value, 0)
        p.easyplot(lat, lon, value, ptype=None, vmin=0, vmax=32767, markersize=0.1, marker='o')
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

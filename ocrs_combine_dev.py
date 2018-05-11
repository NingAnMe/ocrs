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
    else:
        with time_block("all combine"):
            combine = Combine()  # 初始化一个投影实例
            combine.load_yaml_config(config_file)  # 加载配置文件
            with time_block("combine"):
                combine.combine()
            with time_block("write"):
                combine.write()
            with time_block("draw"):
                combine.draw()


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
        self.ofile = None

        self.in_data = {}
        self.attrs = {}
        self.out_data = {}

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

        self.ifile = cfg['PATH']['ipath']
        self.ofile = cfg['PATH']['opath']

        self.res = cfg['PROJ']['res']

        half_res = deg2meter(self.res) / 2.
        self.cmd = cfg['PROJ']['cmd'] % (half_res, half_res)

        self.col = cfg['PROJ']['col']
        self.row = cfg['PROJ']['row']

    def _load_data(self, hdf5_file):
        error = False
        # 加载数据
        if os.path.isfile(hdf5_file):
            try:
                with h5py.File(hdf5_file, 'r') as h5:
                    if len(self.out_data.keys()) == 0:
                        for k in h5.keys():
                            if k == "Longitude" or k == "Latitude":
                                continue
                            self.out_data[k] = h5.get(k)[:]
                            self.attrs[k] = attrs2dict(h5.get(k).attrs)

                    else:
                        for k in h5.keys():
                            self.in_data[k] = h5.get(k)[:]

            except Exception as why:
                print why
                print "Can't open file: {}".format(hdf5_file)
                error = True
                return error
        else:
            print "File does not exist: {}".format(hdf5_file)
            error = True
            return error
        return error

    def combine(self):
        error = False
        # 合成日数据
        if len(self.ifile) < 2:
            return
        filter_value = -999.
        # todo 开发测试使用的 self.ifile,业务要注释掉下面这个语句
        self.ifile = [
            "/storage-space/disk3/Granule/out_del_cloudmask/2017/201701/20170101/20170101_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20170101_{:0>4}_1000M_PROJ.HDF".format(
                x, x) for x in xrange(0, 9999)]
        for in_file in self.ifile:
            if os.path.isfile(in_file):
                print in_file
            else:
                continue
            with time_block("one combine"):
                print '-' * 100
                with time_block("load data"):
                    error = self._load_data(in_file)  # 加载一个时次的数据
                if len(self.in_data.keys()) == 0 or error:
                    continue
                else:
                    for key in self.out_data.keys():
                        if key in self.in_data.keys():
                            value2 = self.in_data[key]
                            idx = np.where(value2 > 0)  # 后面时次的数据覆盖前面时次的数据
                            # idx = np.logical_and(value2 != filter_value, value2 != 0)
                            print len(idx[0]), key
                            self.out_data[key][idx] = value2[idx]
                        else:
                            continue
        with time_block("grid to lons and lats"):
            if "Longitude" not in self.out_data.keys():
                lookup_table = prj_core(self.cmd, self.res, unit="deg", row=self.row, col=self.col)
                lookup_table.grid_lonslats()
                self.out_data["Longitude"] = lookup_table.lons
                self.out_data["Latitude"] = lookup_table.lats

    def project(self, look_table):
        # 进行投影
        ii = look_table.lut_i
        jj = look_table.lut_j

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

        if "Longitude" not in self.out_data.keys():
            look_table.grid_lonslats()
            self.out_data["Longitude"] = look_table.lons
            self.out_data["Latitude"] = look_table.lats

    def write(self):
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

    def draw(self):
        dataset_name = "Ocean_Aod_550"
        out_png_path = os.path.dirname(self.ofile)
        out_png = os.path.join(out_png_path, 'test.png')

        p = dv_map.dv_map()
        # p.title = u'%s_%s %s_%s IR%d tbb (calType %d)' % (sat1, sat2, date, hm, i, calType)
        p.title = "{}    {}".format(dataset_name, self.ymd)
        # out_png = os.path.join(OutPath, '%s_vs_%s_COMB_CalType_%d_%s_%s_IR%d.png' % (sat1, sat2, calType, date, hm, i))

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


def draw_combine(in_file, dataset_name, pic_name, vmin=0, vmax=32767):
    """
    通过日合成文件，画数据集的全球分布图
    :param in_file:
    :param dataset_name:
    :param pic_name:
    :param vmin:
    :param vmax:
    :return:
    """
    if os.path.isfile(in_file):
        print in_file
    else:
        return
    with h5py.File(in_file, 'r') as h5:
        value = h5.get(dataset_name)[:]
        lat = h5.get("Latitude")[:]
        lon = h5.get("Longitude")[:]

    idx = np.where(value > 0)
    print len(idx), dataset_name

    p = dv_map.dv_map()
    p.title = dataset_name
    out_png = pic_name
    # 增加省边界
    #       p1.show_china_province = True
    p.delat = 30
    p.delon = 30
    p.show_line_of_latlon = False
    value = np.ma.masked_less_equal(value, 0)
    p.easyplot(lat, lon, value, ptype=None, vmin=vmin, vmax=vmax, markersize=0.1, marker='o')
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

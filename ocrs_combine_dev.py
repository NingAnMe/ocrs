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
            if combine.error:
                return
            with time_block("write"):
                combine.write()
            with time_block("draw"):
                dataset_name = "Ocean_Aod_550"
                combine.draw(dataset_name)


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

        self.ifile = cfg['PATH']['ipath']
        self.pfile = cfg['PATH']['ppath']
        self.ofile = cfg['PATH']['opath']

        self.res = cfg['PROJ']['res']

        half_res = deg2meter(self.res) / 2.
        self.cmd = cfg['PROJ']['cmd'] % (half_res, half_res)

        self.col = cfg['PROJ']['col']
        self.row = cfg['PROJ']['row']

    def load_proj_data(self, hdf5_file):
        self.error = False
        # 加载数据
        if os.path.isfile(hdf5_file):
            try:
                with h5py.File(hdf5_file, 'r') as h5:
                    self.index_ii = h5.get("index_ii")[:]
                    self.index_jj = h5.get("index_jj")[:]
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
        self.error = False
        # 合成日数据
        if len(self.ifile) < 2:
            return
        fillvalue = -32767.
        # todo 开发测试使用的 self.ifile,业务要注释掉下面这个语句
        self.ifile = [
            "/storage-space/disk3/Granule/out_del_cloudmask/2017/201701/20170101/20170101_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20170101_{:0>4}_1000M.HDF".format(
                x, x) for x in xrange(0, 2500)]
        self.pfile = [
            "/storage-space/disk3/Granule/out_del_cloudmask/2017/201701/20170101/20170101_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20170101_{:0>4}_1000M_PROJ.HDF".format(
                x, x) for x in xrange(0, 2500)]
        self.ifile = [
            "/storage-space/disk3/Granule/out_del_cloudmask/2017/201710/20171012/20171012_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20171012_{:0>4}_1000M.HDF".format(
                x, x) for x in xrange(0, 2500)]
        self.pfile = [
            "/storage-space/disk3/Granule/out_del_cloudmask/2017/201710/20171012/20171012_{:0>4}_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20171012_{:0>4}_1000M_PROJ.HDF".format(
                x, x) for x in xrange(0, 2500)]
        for file_idx, in_file in enumerate(self.ifile):
            proj_file = self.pfile[file_idx]
            if os.path.isfile(in_file) and os.path.isfile(proj_file):
                print in_file, "\n", proj_file
            else:
                print "File is not exist:"
                print in_file, "\n", proj_file
                continue

            # 加载 proj 数据
            self.load_proj_data(proj_file)
            # 日合成
            with time_block("one combine"):
                try:
                    with h5py.File(in_file, 'r') as h5:
                        for k in h5.keys():
                            if k == "Longitude" or k == "Latitude":
                                continue
                            elif k not in self.out_data.keys():
                                self.out_data[k] = np.full((self.row, self.col), fillvalue, dtype='f4')

                            # 合并一个数据
                            proj_data = h5.get(k)[:]
                            self.out_data[k][self.index_ii, self.index_jj] = proj_data[self.data_ii, self.data_jj]

                            # 记录属性信息
                            if k not in self.attrs.keys():
                                self.attrs[k] = attrs2dict(h5.get(k).attrs)
                    print '-' * 100

                except Exception as why:
                    print why
                    print "Can't open file: {}".format(in_file)
                    self.error = True
                    return

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

    def draw(self, dataset_name):
        # out_png_path = os.path.dirname(self.ofile)
        out_png = self.ofile.replace("_COMBINE.HDF", "_COMBINE_{}.png".format(dataset_name))
        print out_png

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
        lat = self.out_data["Latitude"]
        lon = self.out_data["Longitude"]

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
        lats = h5.get("Latitude")[:]
        lons = h5.get("Longitude")[:]

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
    p.easyplot(lats, lons, value, ptype=None, vmin=vmin, vmax=vmax, markersize=0.1, marker='o')
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

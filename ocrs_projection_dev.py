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

    for idx_file, in_file in enumerate(ifile):
        with time_block("one projection"):
            projection = Projection(in_file)  # 初始化一个投影实例
            projection.load_yaml_config(config_file)  # 加载配置文件
            with time_block("load data"):
                projection.load_data()
            with time_block("create lut"):
                projection.create_lut()  # 创建投影查找表
            with time_block("project"):
                projection.project()
            with time_block("write"):
                projection.write(ofile[idx_file])



    # queue_p = Manager().Queue()
    # queue_c = Manager().Queue()
    #
    # with time_block("all projection"):
    #     pool.apply_async(_combine, (combine, queue_p, queue_c))
    #     for in_file in ifile:
    #         pool.apply_async(_projection, (in_file, config_file, queue_p))
    #     pool.close()
    #     pool.join()
    #     print "all projection is done"
    #
    # with time_block("all combine"):
    #     for i in xrange(queue_p.qsize()):
    #         projection = queue_p.get()
    #         combine.in_data = projection.in_data
    #         combine.in_attrs = projection.in_attrs
    #         with time_block("one comb"):
    #             combine.project(projection.lookup_table)  # 进行投影合成

    # 将合成文件写入文件
    # with time_block("write"):
    #     combine = queue_c.get()
    #     combine.write()

    # # 绘图
    # with time_block("draw"):
    #     combine.draw()


def _projection(in_file, config, queue):
    projection = Projection(in_file)  # 初始化一个投影实例
    projection.load_yaml_config(config)  # 加载配置文件
    with time_block("load data"):
        projection.load_data()
    with time_block("create lut"):
        projection.create_lut()  # 创建投影查找表
    queue.put(projection)


def _combine(combine, queue_p, queue_c):
    """
    对投影数据进行合并
    :return:
    """
    while True:
        if not queue_p.empty():
            projection = queue_p.get()
        else:
            continue

        combine.in_data = projection.in_data
        combine.in_attrs = projection.in_attrs
        with time_block("proj"):
            combine.project(projection.lookup_table)  # 进行投影合成




class Projection(object):

    def __init__(self, in_file):
        self.file = in_file

        self.sat = None
        self.sensor = None
        self.ymd = None

        self.cmd = None
        self.col = None
        self.row = None
        self.res = None

        self.in_data = {}
        self.in_attrs = {}
        self.out_data = {}

        self.lookup_table = None

    def load_yaml_config(self, in_proj_cfg):
        """
        读取 yaml 格式配置文件
        """
        if not os.path.isfile(in_proj_cfg):
            print 'Not Found %s' % in_proj_cfg
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
                        self.in_attrs[k] = attrs2dict(h5.get(k).attrs)
            except Exception as why:
                print why
                print "Can't open file: {}".format(self.file)
                return
        else:
            print "File does not exist: {}".format(self.file)
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
                    fillValue = -32767.
                    self.out_data[k] = np.full((self.row, self.col), fillValue, dtype='f4')
                    self.out_data[k][idx] = self.in_data[k][ii[idx], jj[idx]]

        if "Longitude" not in self.out_data.keys():
            self.lookup_table.grid_lonslats()
            self.out_data["Longitude"] = self.lookup_table.lons
            self.out_data["Latitude"] = self.lookup_table.lats

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
                attrs = self.in_attrs[k]
                for key, value in attrs.items():
                    h5[k].attrs[key] = value


class Combine(object):

    def __init__(self):
        self.col = None
        self.row = None

        self.ofile = None

        self.in_data = {}
        self.in_attrs = {}
        self.out_data = {}

    def load_yaml_config(self, in_proj_cfg):
        """
        读取 yaml 格式配置文件
        """
        if not os.path.isfile(in_proj_cfg):
            print 'Not Found %s' % in_proj_cfg
            return

        with open(in_proj_cfg, 'r') as stream:
            cfg = yaml.load(stream)

        self.ofile = cfg['PATH']['opath']

        self.col = cfg['PROJ']['col']
        self.row = cfg['PROJ']['row']

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
                    fillValue = -32767.
                    self.out_data[k] = np.full((self.row, self.col), fillValue, dtype='f4')
                    self.out_data[k][idx] = self.in_data[k][ii[idx], jj[idx]]

        if "Longitude" not in self.out_data.keys():
            look_table.grid_lonslats()
            self.out_data["Longitude"] = look_table.lons
            self.out_data["Latitude"] = look_table.lats

    def write(self):
        # 写入 HDF5 文件
        with h5py.File(self.ofile, 'w') as h5:
            for k in self.out_data.keys():
                # 创建数据集
                h5.create_dataset(k, dtype='f4',
                                  data=self.out_data[k],
                                  compression='gzip', compression_opts=5,
                                  shuffle=True)
                # 复制属性
                attrs = self.in_attrs[k]
                for key, value in attrs.items():
                    h5[k].attrs[key] = value

    def draw(self):
        p = dv_map.dv_map()
        # p.title = u'%s_%s %s_%s IR%d tbb (calType %d)' % (sat1, sat2, date, hm, i, calType)
        p.title = "test"
        # out_png = os.path.join(OutPath, '%s_vs_%s_COMB_CalType_%d_%s_%s_IR%d.png' % (sat1, sat2, calType, date, hm, i))
        out_png = 'test.png'
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
        value = self.out_data["Ocean_Aod_550"]
        value = np.ma.masked_less_equal(value, 0)
        p.easyplot(lat, lon, value, ptype=None, vmin=0, vmax=32767, markersize=0.1, marker='o')
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

    if args == 1:
        with time_block("all"):
            run(args[0])
    else:
        print help_info

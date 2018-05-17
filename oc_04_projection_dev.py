# coding:utf-8
"""
ocrs_projection.py
将水色产品在全球进行投影，获取投影后的位置信息和数据的位置查找表信息
~~~~~~~~~~~~~~~~~~~
creation time : 2018 5 9
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
from DV import dv_map
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
    else:
        # 加载配置信息
        try:
            DRAW_INFO = proj_cfg['project'][pair]['draw']
        except ValueError:
            log.error("Load yaml config file error, please check it. : {}".format(proj_cfg_file))
            return

    ######################### 开始处理 ###########################
    # 加载 colloc 文件内容
    if not os.path.isfile(colloc_file):
        log.error("File is not exist: {}".format(colloc_file))
        return
    else:
        try:
            with open(colloc_file, 'r') as stream:
                cfg = yaml.load(stream)
                ifile = cfg['PATH']['ipath']
                pfile = cfg['PATH']['ppath']

                res = cfg['PROJ']['res']

                half_res = deg2meter(res) / 2.
                cmd = cfg['PROJ']['cmd'] % (half_res, half_res)

                col = cfg['PROJ']['col']
                row = cfg['PROJ']['row']
                if ifile is None or pfile is None:
                    log.error("Is None: ifile or pfile".format(colloc_file))
                    return
        except Exception as why:
            print why
            log.error("Load colloc file error, please check it. : {}".format(colloc_file))
            return

    # 创建查找表
    lookup_table = prj_core(cmd, res, unit="deg", row=row, col=col)

    # 对数据列表进行循环处理
    for idx_file, in_file in enumerate(ifile):
        print "-" * 100

        if not os.path.isfile(in_file):
            log.error("File is not exist: {}".format(in_file))
            continue
        else:
            print "Input file: {}".format(in_file)
        with time_block("One project time:", switch=TIME_TEST):
            out_file = pfile[idx_file]
            projection = Projection()
            projection.load_yaml_config(colloc_file)
            projection.project(in_file, out_file, lookup_table)

        with time_block("Draw one projection picture time:", switch=TIME_TEST):
            if DRAW_INFO is not None:
                for info in DRAW_INFO:
                    dataset_name = info[0]
                    projection.draw(in_file, out_file, dataset_name)

        print "-" * 100


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
        self.lut_ii = None
        self.lut_jj = None
        self.data_ii = None
        self.data_jj = None

    def load_yaml_config(self, in_proj_cfg):
        """
        读取 yaml 格式配置文件
        """
        if self.error:
            return
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
        if self.error:
            return
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
        if self.error:
            return
        # 通过查找表和经纬度数据生成数据在全球的行列信息
        lookup_table.create_lut(self.lons, self.lats)
        self.ii = lookup_table.lut_i
        self.jj = lookup_table.lut_j

    def _get_index(self):
        if self.error:
            return
        # 获取数据的索引信息
        self.index = np.logical_and(self.ii >= 0, self.jj >= 0)
        self.index = np.where(self.index)  # 全球投影的行列索引信息
        self.lut_ii = self.index[0].T
        self.lut_jj = self.index[1].T
        self.data_ii = self.ii[self.index]  # 数据行索引信息
        self.data_jj = self.jj[self.index]  # 数据列索引信息

    def _write(self, out_file):
        if self.error:
            return
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
            h5.create_dataset("lut_ii", dtype='i2',
                              data=self.lut_ii,
                              compression='gzip', compression_opts=1,
                              shuffle=True)
            h5.create_dataset("lut_jj", dtype='i2',
                              data=self.lut_jj,
                              compression='gzip', compression_opts=1,
                              shuffle=True)
        print "Output file: {}".format(out_file)

    def project(self, in_file, out_file, lookup_table):
        with time_block("Load lons and lats time:", switch=TIME_TEST):
            # 加载经纬度数据
            self._load_lons_lats(in_file)
        with time_block("Create lut time:", switch=TIME_TEST):
            # 使用查找表生成经纬度对应的行列信息
            self._create_lut(lookup_table)
        with time_block("Get index time:", switch=TIME_TEST):
            # 使用生成的行列信息生成数据的索引信息
            self._get_index()
        with time_block("Write data time:", switch=TIME_TEST):
            # 将数据的索引信息和在全球的行列信息进行写入
            self._write(out_file)

    def write(self, out_file):
        if self.error:
            return
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
        print "Output file: {}".format(out_file)

    def draw(self, in_file, proj_file, dataset_name, vmin=None, vmax=None):
        if self.error:
            return
        # 加载 Proj 数据
        if os.path.isfile(proj_file):
            try:
                with h5py.File(proj_file, 'r') as h5:
                    lut_ii = h5.get("lut_ii")[:]
                    lut_jj = h5.get("lut_jj")[:]
                    data_ii = h5.get("data_ii")[:]
                    data_jj = h5.get("data_jj")[:]
            except Exception as why:
                print why
                print "Can't open file: {}".format(proj_file)
                return
        else:
            print "File does not exist: {}".format(proj_file)
            return

        with time_block("Draw load", switch=TIME_TEST):
            # 加载产品数据
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

        if vmin is not None:
            vmin = vmin
        if vmax is not None:
            vmax = vmax

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
        fillValue = -32767
        value = np.full((self.row, self.col), fillValue, dtype='f4')

        value[lut_ii, lut_jj] = proj_value
        value = np.ma.masked_less_equal(value, 0)  # 掩掉 <=0 的数据

        # 乘数据的系数，水色产品为 0.001
        slope = 0.001
        value = value * slope

        p.easyplot(lats, lons, value, ptype=None, vmin=vmin, vmax=vmax, markersize=0.1, marker='o')

        out_png_path = os.path.dirname(in_file)
        out_png = os.path.join(out_png_path, '{}.png'.format(dataset_name))
        pb_io.make_sure_path_exists(os.path.dirname(out_png))
        p.savefig(out_png, dpi=300)
        print "Output picture: {}".format(out_png)


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
        [样例]： python ocrs_projection.py FY3B+MERSI 20171012.colloc
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

        with time_block("Project time:", switch=TIME_TEST):
            run(sat_sensor, file_path)
        # pool.apply_async(run, (sat_sensor, file_path))
        # pool.close()
        # pool.join()

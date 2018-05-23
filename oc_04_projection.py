# coding:utf-8
"""
将水色产品在全球进行投影，获取投影后的位置信息和数据的位置查找表信息
存储在 HDF5 文件
~~~~~~~~~~~~~~~~~~~
creation time : 2018 5 9
author : anning
~~~~~~~~~~~~~~~~~~~
"""
import os
import sys

import h5py
import numpy as np
from configobj import ConfigObj

from DP.dp_prj_new import prj_core
from DV import dv_map
from PB import pb_io, pb_time
from PB.pb_time import time_block
from PB.pb_space import deg2meter
from PB.CSC.pb_csc_console import LogServer


TIME_TEST = True  # 时间测试


def run(sat_sensor, in_file):
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
    if not os.path.isfile(in_file):
        LOG.error("File is not exist: {}".format(in_file))
        return
    else:
        print "-" * 100
        try:
            with time_block("One project time:", switch=TIME_TEST):
                # 生成输出文件的文件名
                ymd = pb_time.get_ymd(in_file)
                hms = pb_time.get_hm(in_file)
                sat, sensor = sat_sensor.split('+')
                out_name = "{}_{}_ORBT_L2_ASO_MLT_NUL_{}_{}_{}.HDF".format(sat, sensor, ymd, hms, MESH_SIZE.upper())
                out_path = pb_io.path_replace_ymd(OUT_PATH, ymd)
                out_file = os.path.join(out_path, out_name)

                # 如果输出文件已经存在，跳过
                if os.path.isfile(out_file):
                    print "File is already exist, skip it: {}".format(out_file)
                    return

                projection = Projection(cmd=CMD, row=ROW, col=COL, res=RES)
                # 开始创建投影查找表
                projection.project(in_file, out_file)
        except Exception as why:
            print why
            LOG.error("Projection had some errors: {}".format(in_file))
            return

        print "-" * 100


class Projection(object):

    def __init__(self, cmd=None, row=None, col=None, res=None, sat_sensor=None, ymd=None):
        self.error = False

        if sat_sensor is not None:
            self.sat, self.sensor = sat_sensor.split("+")
        else:
            self.sat = self.sensor = None

        self.ymd = ymd if ymd is not None else None

        self.cmd = cmd
        self.row = row
        self.col = col
        self.res = res

        self.in_data = {}
        self.attrs = {}
        self.out_data = {}

        self.lons = None
        self.lats = None

        self.ii = None  # 查找表行矩阵
        self.jj = None  # 查找表列矩阵
        self.lut_ii = None  # 查找表行索引信息
        self.lut_jj = None  # 查找表列索引信息
        self.data_ii = None  # 数据行索引信息
        self.data_jj = None  # 数据列索引信息

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

    def _create_lut(self):
        if self.error:
            return
        # 创建查找表
        lookup_table = prj_core(self.cmd, self.res, unit="deg", row=self.row, col=self.col)
        # 通过查找表和经纬度数据生成数据在全球的行列信息
        lookup_table.create_lut(self.lons, self.lats)
        self.ii = lookup_table.lut_i
        self.jj = lookup_table.lut_j

    def _get_index(self):
        if self.error:
            return
        # 获取数据的索引信息
        idx = np.logical_and(self.ii >= 0, self.jj >= 0)  # 行列 >= 0 说明投在地图上面
        idx = np.where(idx)  # 全球投影的行列索引信息
        print len(idx[0])
        self.lut_ii = idx[0].T
        self.lut_jj = idx[1].T
        self.data_ii = self.ii[idx]  # 数据行索引信息
        self.data_jj = self.jj[idx]  # 数据列索引信息

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

    def project(self, in_file, out_file):
        with time_block("Load lons and lats time:", switch=TIME_TEST):
            # 加载经纬度数据
            self._load_lons_lats(in_file)
        with time_block("Create lut time:", switch=TIME_TEST):
            # 使用查找表生成经纬度对应的行列信息
            self._create_lut()
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


######################### 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    ARGS = sys.argv[1:]
    HELP_INFO = \
        u"""
        [arg1]：hdf_file
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
        OUT_PATH = IN_CFG["PATH"]["MID"]["projection"]

        with time_block("Project time:", switch=TIME_TEST):
            run(SAT_SENSOR, FILE_PATH)
        # pool.apply_async(run, (sat_sensor, file_path))
        # pool.close()
        # pool.join()

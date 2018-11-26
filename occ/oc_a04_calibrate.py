#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/10/22
@Author  : AnNing
"""
import os
from datetime import datetime

import re
import sys

import h5py
import numpy as np

from DV.dv_img import dv_rgb
from PB import pb_io
from PB.CSC.pb_csc_console import LogServer
from PB.pb_io import path_replace_ymd
from PB import pb_time, pb_name
from PB.pb_time import time_block
from app.config import InitApp

TIME_TEST = False  # 时间测试


def main(sat_sensor, in_file):
    """
    使用矫正系数对 MERSI L1 的产品进行定标预处理
    :param sat_sensor: (str) 卫星对
    :param in_file: (str) 输入文件
    :return:
    """
    # ######################## 初始化 ###########################
    # 获取程序所在位置，拼接配置文件
    app = InitApp(sat_sensor)
    if app.error:
        print "Load config file error."
        return

    gc = app.global_config
    sc = app.sat_config

    log = LogServer(gc.path_out_log)

    # 全局配置接口
    obc_path = gc.path_in_obc
    coeff_path = gc.path_in_coeff
    out_path = gc.path_mid_calibrate
    # 程序配置接口

    # 卫星配置接口
    launch_date = sc.launch_date
    plot = sc.calibrate_plot

    # ######################## 开始处理 ###########################
    print '-' * 100
    print 'Start calibration'

    # 获取 M1000 文件和对应 OBC 文件
    l1_1000m = in_file
    obc_1000m = _get_obc_file(l1_1000m, obc_path)
    if not os.path.isfile(l1_1000m):
        log.error("File is not exist: {}".format(l1_1000m))
        return
    elif not os.path.isfile(obc_1000m):
        log.error("File is not exist: {}".format(obc_1000m))
        return
    else:
        print "<<< {}".format(l1_1000m)
        print "<<< {}".format(obc_1000m)

    ymd = _get_ymd(l1_1000m)

    # 获取 coefficient 水色波段系统定标系数， 2013年以前和2013年以后不同
    coeff_file = os.path.join(coeff_path, '{}.txt'.format(ymd[0:4]))
    if not os.path.isfile(coeff_file):
        log.info("Coeff File is not exist: {}".format(coeff_file))
        coeff_file = None
    else:
        print "<<< {}".format(coeff_file)

    # 获取输出文件
    out_path = pb_io.path_replace_ymd(out_path, ymd)
    _name = os.path.basename(l1_1000m)
    out_file = os.path.join(out_path, _name)

    # 如果输出文件已经存在，跳过预处理
    if os.path.isfile(out_file):
        print "File is already exist, skip it: {}".format(out_file)
        return

    # 初始化一个预处理实例
    calibrate = CalibrateFY3BTemporary(l1_1000m=l1_1000m, obc_1000m=obc_1000m,
                                       coeff_file=coeff_file,
                                       out_file=out_file, launch_date=launch_date)

    # 重新定标 L1 数据
    calibrate.calibrate()

    # 将新数据写入 HDF5 文件
    calibrate.write()

    if not calibrate.error:
        print ">>> {}".format(calibrate.out_file)

        # 对原数据和处理后的数据各出一张真彩图
        if plot == "on":
            if not calibrate.error:
                picture_suffix = "650_565_490"
                file_name = os.path.splitext(out_file)[0]
                out_pic_old = "{}_{}_old.{}".format(file_name, picture_suffix, "png")
                out_pic_new = "{}_{}_new.{}".format(file_name, picture_suffix, "png")
                # 如果输出文件已经存在，跳过
                if os.path.isfile(out_pic_old):
                    print "File is already exist, skip it: {}".format(out_pic_old)
                    return
                else:
                    _plot_rgb_fy3b_old(in_file, out_pic_old)
                if os.path.isfile(out_pic_new):
                    print "File is already exist, skip it: {}".format(out_pic_new)
                    return
                else:
                    _plot_rgb_fy3b_new(out_file, out_pic_new)
    else:
        print "Error: Calibrate error".format(in_file)


def _get_ymd(l1_file):
    """
    从输入的L1文件中获取 ymd
    :param l1_file:
    :return:
    """
    if not isinstance(l1_file, str):
        return
    m = re.match(r".*_(\d{8})_", l1_file)

    if m is None:
        return
    else:
        return m.groups()[0]


def _get_hm(l1_file):
    """
    从输入的L1文件中获取 ymd
    :param l1_file:
    :return:
    """
    if not isinstance(l1_file, str):
        return
    m = re.match(r".*_(\d{4})_", l1_file)

    if m is None:
        return
    else:
        return m.groups()[0]


def _plot_rgb_fy3b_old(l1_file, out_file):
    """
    对原数据和处理后的数据各出一张真彩图
    """
    try:
        with h5py.File(l1_file) as h5:
            r = h5.get("EV_1KM_RefSB")[5]  # 第 11 通道 650
            g = h5.get("EV_1KM_RefSB")[4]  # 第 10 通道 565
            b = h5.get("EV_1KM_RefSB")[2]  # 第 8 通道 490
        dv_rgb(r, g, b, out_file)
        print ">>> {}".format(out_file)
    except Exception as why:
        print why
        print "Error: plot RGB error".format(l1_file)
        return


def _plot_rgb_fy3b_new(l1_file, out_file):
    """
    对原数据和处理后的数据各出一张真彩图
    """
    try:
        with h5py.File(l1_file) as h5:
            r = h5.get("CH_11/Ref")[:]  # 第 11 通道 650
            g = h5.get("CH_10/Ref")[:]  # 第 10 通道 565
            b = h5.get("CH_08/Ref")[:]  # 第 8 通道 490
        dv_rgb(r, g, b, out_file)
        print ">>> {}".format(out_file)
    except Exception as why:
        print why
        print "Error: plot RGB error".format(l1_file)
        return


def _plot_rgb_fy3d_new(l1_file, out_file):
    """
    对原数据和处理后的数据各出一张真彩图
    """
    try:
        with h5py.File(l1_file) as h5:
            r = h5.get("CH_03/Ref")[:]  # 第 8 通道 670
            g = h5.get("CH_02/Ref")[:]  # 第 6 通道 555
            b = h5.get("CH_01/Ref")[:]  # 第 4 通道 490
        dv_rgb(r, g, b, out_file)
        print ">>> {}".format(out_file)
    except Exception as why:
        print why
        print "Error: plot RGB error".format(l1_file)
        return


def _get_obc_file(m1000_file, obc_path):
    """
    通过 1KM 文件路径生成 OBC 文件的路径
    :param m1000_file:
    :param obc_path:
    :return:
    """
    print obc_path
    ymd = _get_ymd(m1000_file)
    hm = _get_hm(m1000_file)
    time = ymd + hm
    obc_path = path_replace_ymd(obc_path, ymd)
    obc_files = get_files_by_ymd(obc_path, time, time,
                                 ext='.HDF',
                                 pattern_ymd=r".*_(\d{8})_(\d{4})_")
    obc_file = obc_files[0]
    return obc_file


def _get_geo_file(m1000_file, geo_path):
    """
    通过 1KM 文件路径生成 GEO 文件的路径
    :param m1000_file:
    :param geo_path:
    :return:
    """
    ymd = _get_ymd(m1000_file)
    hm = _get_hm(m1000_file)
    time = ymd + hm
    geo_path = path_replace_ymd(geo_path, ymd)

    print geo_path
    print time
    geo_files = get_files_by_ymd(geo_path, time, time,
                                 ext='.HDF',
                                 pattern_ymd=r".*_(\d{8})_(\d{4})_")
    geo_file = geo_files[0]
    return geo_file


def get_files_by_ymd(dir_path, time_start, time_end, ext=None, pattern_ymd=None):
    """
    :param dir_path: 文件夹
    :param time_start: 开始时间
    :param time_end: 结束时间
    :param ext: 后缀名
    :param pattern_ymd: 匹配时间的模式
    :return: list
    """
    files_found = []
    if pattern_ymd is not None:
        pattern = pattern_ymd
    else:
        pattern = r".*(\d{8})"

    for root, dirs, files in os.walk(dir_path):
        for file_name in files:
            if ext is not None:
                if os.path.splitext(file_name)[1].lower() != ext.lower():
                    continue
            re_result = re.match(pattern, file_name)
            if re_result is not None:
                time_file = ''.join(re_result.groups())
            else:
                continue
            if int(time_start) <= int(time_file) <= int(time_end):
                files_found.append(os.path.join(root, file_name))
    return files_found


class CalibrateFY3BTemporary(object):
    def __init__(self, l1_1000m=None, obc_1000m=None, coeff_file=None, out_file=None,
                 launch_date=None):
        """
        :param l1_1000m: L1 1000m 文件
        :param obc_1000m: OBC 1000m 文件
        :param coeff_file: 矫正系数文件，txt 格式，三列，分别为 k0 k1 k2
        :param out_file: 输出文件
        :param launch_date: 发星时间
        :return:
        """
        self.error = False
        self.sv_extract_obc = []
        self.ev_1000m_ref = []
        self.ev_250m_ref = []

        self.l1_1000m = l1_1000m
        self.obc_1000m = obc_1000m
        self.coeff_file = coeff_file
        self.out_file = out_file
        self.launch_date = launch_date

        # 分通道数据集，使用 {}
        self.SV = None  # {}
        self.Ref = None  # {}
        self.BB = None  # {}
        self.CalCoeff = None  # {}

        # 不分通道数据集，使用 []
        self.Latitude = None  # []
        self.Longitude = None  # []
        self.SensorAzimuth = None  # []
        self.SensorZenith = None  # []
        self.SolarAzimuth = None  # []
        self.SolarZenith = None  # []
        self.LandCover = None  # []
        self.LandSeaMask = None  # []
        self.Time = None  # [] 转成距离1970年的秒

        self.coeff = None

        self._get_ymd()
        if self.coeff_file is not None:
            self._get_coeff()
        self._get_dsl()
        self._get_time()

    def _get_ymd(self):
        if self.error:
            return
        try:
            self.ymd = pb_time.get_ymd(self.l1_1000m)
            self.hm = pb_time.get_hm(self.l1_1000m)
        except Exception as why:
            print why
            self.error = True

    def _get_time(self):
        shape = self._get_shape()
        time = np.full(shape, -999.)
        name_class = pb_name.nameClassManager()
        info = name_class.getInstance(os.path.basename(self.l1_1000m))
        secs = int((info.dt_s - datetime(1970, 1, 1, 0, 0, 0)).total_seconds())
        time[:] = secs
        if not self.Time:
            self.Time = time
        else:
            self.Time = np.concatenate((self.Time, time))

    def _get_coeff(self):
        if self.error:
            return
        if not os.path.isfile(self.coeff_file):
            print "File is not exist: {}".format(self.coeff_file)
            self.error = True
            return
        try:
            self.coeff = np.loadtxt(self.coeff_file)
        except Exception as why:
            print why
            self.error = True

    def _get_dsl(self):
        if self.error:
            return
        try:
            self.dsl = pb_time.get_dsl(self.ymd, self.launch_date)
        except Exception as why:
            print why
            self.error = True

    def _get_shape(self):
        with h5py.File(self.l1_1000m) as hdf5:
            self.shape = hdf5.get('Latitude').shape
        return self.shape

    def calibrate(self):
        # 定标计算
        # 发星-201303060015
        if int(self.ymd + self.hm) <= 201303060015:
            for i in xrange(19):
                if i < 4:
                    ev_name = "EV_250_Aggr.1KM_RefSB"
                    k = i
                else:
                    ev_name = "EV_1KM_RefSB"
                    k = i - 4

                with h5py.File(self.l1_1000m, "r") as h5:
                    ev_dn_l1 = h5.get(ev_name)[:][k]
                    ev_slope = h5.get(ev_name).attrs.get("Slope")[k]
                    ev_intercept = h5.get(ev_name).attrs.get("Intercept")[k]

                # 除去有效范围外的 dn 值
                ev_dn_l1 = np.ma.masked_less_equal(ev_dn_l1, 0)
                ev_dn_l1 = np.ma.masked_greater(ev_dn_l1, 4095)

                # 进行计算
                if self.coeff is not None:
                    k0, k1, k2 = self.coeff[i]
                    ev_dn_l1 = ev_dn_l1 * ev_slope + ev_intercept
                    slope = (self.dsl ** 2) * k2 + self.dsl * k1 + k0
                    ref_new = ev_dn_l1 * slope * 100
                else:
                    ref_new = ev_dn_l1

                # 除去有效范围外的 dn 值
                ref_new = np.ma.masked_less_equal(ref_new, 0)
                ref_new = ref_new.filled(0)
                ref_new = ref_new.astype(np.uint16)

                if i < 4:
                    self.ev_250m_ref.append(ref_new)
                else:
                    self.ev_1000m_ref.append(ref_new)

        # 201303060020 - 今
        else:
            for i in xrange(19):
                if i < 4:
                    k = i
                    ev_name = "EV_250_Aggr.1KM_RefSB"
                    sv_name = "SV_250_Aggr1KM_RefSB"
                else:
                    k = i - 4
                    ev_name = "EV_1KM_RefSB"
                    sv_name = "SV_1KM_RefSB"

                with h5py.File(self.l1_1000m, "r") as h5:
                    ev_ref_l1 = h5.get(ev_name)[:][k]
                    ev_slope = h5.get(ev_name).attrs.get("Slope")[k]
                    ev_intercept = h5.get(ev_name).attrs.get("Intercept")[k]
                    sv_dn_l1 = h5.get(sv_name)[:][k]
                    coeff_old = h5.get('RSB_Cal_Cor_Coeff')[:]

                k0_old, k1_old, k2_old = coeff_old[i]

                # 除去有效范围外的 dn 值
                ev_ref_l1 = np.ma.masked_less_equal(ev_ref_l1, 0)
                ev_ref_l1 = np.ma.masked_greater(ev_ref_l1, 10000)

                # 进行计算
                if self.coeff is not None:
                    k0_new, k1_new, k2_new = self.coeff[i]
                    ev_ref_l1 = ev_ref_l1 * ev_slope + ev_intercept
                    slope_old = (self.dsl ** 2) * k2_old + self.dsl * k1_old + k0_old
                    dn_new = ev_ref_l1 / slope_old + sv_dn_l1
                    slope_new = (self.dsl ** 2) * k2_new + self.dsl * k1_new + k0_new
                    ref_new = dn_new * slope_new * 100
                else:
                    ev_ref_l1 = ev_ref_l1 * ev_slope + ev_intercept
                    ref_new = ev_ref_l1

                # 除去有效范围外的 dn 值
                ref_new = np.ma.masked_less_equal(ref_new, 0)
                ref_new = ref_new.filled(0)
                ref_new = ref_new.astype(np.uint16)

                if i < 4:
                    self.ev_250m_ref.append(ref_new)
                else:
                    self.ev_1000m_ref.append(ref_new)

    @staticmethod
    def _create_dataset(name, data, dtype, hdf5, compression='gzip', compression_opts=1,
                        shuffle=True):
        dataset = hdf5.create_dataset(name, data=data, dtype=dtype,
                                      compression=compression,
                                      compression_opts=compression_opts,
                                      shuffle=shuffle)
        return dataset

    def write(self):
        """
        将处理后的数据写入 HDF5 文件
        """
        # 创建生成输出目录
        pb_io.make_sure_path_exists(os.path.dirname(self.out_file))
        # 写入数据
        with h5py.File(self.out_file, 'w') as hdf5:
            with h5py.File(self.l1_1000m, 'r') as m1000:
                with h5py.File(self.obc_1000m, 'r') as obc:
                    # 创建输出文件的数据集
                    for i in xrange(0, 20):
                        channel_name = 'CH_{:02}'.format(i + 1)

                        name = '{}/Ref'.format(channel_name)
                        if i < 4:
                            data = self.ev_250m_ref[i]
                        elif i == 4:
                            data = m1000.get('EV_250_Aggr.1KM_Emissive')[:]
                        else:
                            i = i - 5
                            data = self.ev_1000m_ref[i]
                        dtype = 'u2'
                        self._create_dataset(name, data, dtype, hdf5)

                        name = '{}/SV'.format(channel_name)
                        if i < 4:
                            data = obc.get('SV_250m_REFL')[i]
                        elif i == 4:
                            data = obc.get('SV_250m_EMIS')[:]
                        else:
                            i = i - 1
                            data = obc.get('SV_1km')[i]
                        dtype = 'u2'
                        self._create_dataset(name, data, dtype, hdf5)

                        name = '{}/CalCoeff'.format(channel_name)
                        if self.coeff is None:
                            data = np.array([1., 0., 0.]).reshape(-1, 1)
                        else:
                            if i < 4:
                                data = self.coeff[i].reshape(-1, 1)
                            elif i == 4:
                                data = np.array([1., 0., 0.]).reshape(-1, 1)
                            else:
                                i = i - 1
                                data = self.coeff[i].reshape(-1, 1)
                        dtype = 'f4'
                        self._create_dataset(name, data, dtype, hdf5)

                        name = '{}/BB'.format(channel_name)
                        data = m1000.get('BB_DN_average')[i].reshape(-1, 1)
                        dtype = 'f4'
                        self._create_dataset(name, data, dtype, hdf5)

                    name = 'Height'
                    dtype = 'i2'
                    data = m1000.get('Height')[:]
                    self._create_dataset(name, data, dtype, hdf5)

                    name = 'LandCover'
                    dtype = 'u1'
                    data = m1000.get('LandCover')[:]
                    self._create_dataset(name, data, dtype, hdf5)
                    name = 'LandSeaMask'
                    dtype = 'u1'
                    data = m1000.get('LandSeaMask')[:]
                    self._create_dataset(name, data, dtype, hdf5)

                    name = 'Latitude'
                    dtype = 'f4'
                    data = m1000.get('Latitude')[:]
                    self._create_dataset(name, data, dtype, hdf5)
                    name = 'Longitude'
                    dtype = 'f4'
                    data = m1000.get('Longitude')[:]
                    self._create_dataset(name, data, dtype, hdf5)

                    name = 'SolarZenith'
                    dtype = 'i2'
                    data = m1000.get('SolarZenith')[:]
                    self._create_dataset(name, data, dtype, hdf5)
                    name = 'SolarAzimuth'
                    dtype = 'i2'
                    data = m1000.get('SolarAzimuth')[:]
                    self._create_dataset(name, data, dtype, hdf5)
                    name = 'SensorZenith'
                    dtype = 'i2'
                    data = m1000.get('SensorZenith')[:]
                    self._create_dataset(name, data, dtype, hdf5)
                    name = 'SensorAzimuth'
                    dtype = 'i2'
                    data = m1000.get('SensorAzimuth')[:]
                    self._create_dataset(name, data, dtype, hdf5)

                    name = 'Times'
                    dtype = 'i4'
                    data = self.Time
                    self._create_dataset(name, data, dtype, hdf5)

                    # 复制文件属性
                    pb_io.copy_attrs_h5py(m1000, hdf5)

                    # 添加文件属性
                    hdf5.attrs['dsl'] = self.dsl


# ######################## 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    ARGS = sys.argv[1:]
    HELP_INFO = \
        u"""
        [arg1]：sat+sensor
        [arg2]：hdf_file
        [example]： python app.py arg1 arg2
        """
    if "-h" in ARGS:
        print HELP_INFO
        sys.exit(-1)

    if len(ARGS) != 2:
        print HELP_INFO
        sys.exit(-1)
    else:
        SAT_SENSOR = ARGS[0]
        FILE_PATH = ARGS[1]

        with time_block("Calibrate time:", switch=TIME_TEST):
            main(SAT_SENSOR, FILE_PATH)

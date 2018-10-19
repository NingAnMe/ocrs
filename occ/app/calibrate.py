#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/6/7 14:08
@Author  : AnNing
"""
import re
import os
from datetime import datetime

import h5py
import numpy as np

from PB.pb_time import fy3_ymd2seconds
from PB import pb_io, pb_time, pb_calculate
from PB import pb_name


class CalibrateFY3D(object):
    """
    1km的mersi2数据类
    """

    def __init__(self):

        # 定标使用
        self.error = False
        self.shape = (2000, 2048)
        self.geo_file = None
        self.in_file = None
        self.sat = 'FY3D'
        self.sensor = 'MERSI'
        self.res = 1000
        self.Band = 25
        self.orbit_direction = []
        self.orbit_num = []
        self.Dn = {}
        self.Ref = {}
        self.Rad = {}
        self.Tbb = {}

        self.satAzimuth = None
        self.satZenith = None
        self.sunAzimuth = None
        self.sunZenith = None
        self.Lons = None
        self.Lats = None
        self.Time = None

        self.SV = {}
        self.BB = {}
        self.LandSeaMask = None
        self.LandCover = None

        self.ary_height = None
        self.ary_satz = None
        self.ary_sata = None
        self.ary_sunz = None
        self.ary_suna = None
        self.ary_lon = None
        self.ary_lat = None
        self.ary_land_cover = None
        self.ary_land_sea_mask = None

        self.cal_coeff1 = {}

        # 投影使用
        self.VIS_Coeff = []

    def calibrate(self, in_file, geo_file, ary_vis_coeff_file=None):

        self.in_file = in_file
        self.geo_file = geo_file

        with h5py.File(in_file, 'r') as hdf5:
            orbit_direction = hdf5.attrs.get('Orbit Direction')
            orbit_num = hdf5.attrs.get('Orbit Number')
            self.orbit_direction.append(orbit_direction)
            self.orbit_num.append(orbit_num)

            ary_ch1_4 = hdf5.get('/Data/EV_250_Aggr.1KM_RefSB')[:]
            ary_ch5_19 = hdf5.get('/Data/EV_1KM_RefSB')[:]
            if ary_vis_coeff_file:
                ary_vis_coeff = np.loadtxt(ary_vis_coeff_file)
            else:
                ary_vis_coeff = hdf5.get('/Calibration/VIS_Cal_Coeff')[:]
            ary_sv = hdf5.get('/Calibration/SV_DN_average')[:]
            ary_bb = hdf5.get('/Calibration/BB_DN_average')[:]

        with h5py.File(geo_file, 'r') as hdf5:
            self.ary_height = hdf5.get('/Geolocation/DEM')[:]
            self.ary_satz = hdf5.get('/Geolocation/SensorZenith')[:]
            self.ary_sata = hdf5.get('/Geolocation/SensorAzimuth')[:]
            self.ary_sunz = hdf5.get('/Geolocation/SolarZenith')[:]
            self.ary_suna = hdf5.get('/Geolocation/SolarAzimuth')[:]
            self.ary_lon = hdf5.get('/Geolocation/Longitude')[:]
            self.ary_lat = hdf5.get('/Geolocation/Latitude')[:]
            self.ary_land_cover = hdf5.get('/Geolocation/LandCover')[:]
            self.ary_land_sea_mask = hdf5.get('/Geolocation/LandSeaMask')[:]
            ary_day = hdf5.get('/Timedata/Day_Count')[:]
            ary_time = hdf5.get('/Timedata/Millisecond_Count')[:]

        # 1-19通道的可见光数据进行定标
        k = ary_vis_coeff
        for i in range(19):
            if i < 4:
                # 统一下标
                j = i
                indata = ary_ch1_4[j]
            else:
                j = i - 4
                indata = ary_ch5_19[j]

            # 初始存放dn数据的结构，初始值 Nan
            dn = np.full(self.shape, np.nan)
            idx = np.logical_and(indata < 11000, indata >= 0)
            dn[idx] = indata[idx]
            ref = ((dn ** 2 * k[i, 2]) + dn * k[i, 1] + k[i, 0]) * 100.

            # # 除去有效范围外的 ref 值
            ref = np.ma.masked_greater_equal(ref, 11000)
            ref = np.ma.masked_less(ref, 0)
            ref = ref.filled(0)

            channel_name = 'CH_%02d' % (i + 1)
            if channel_name not in self.Dn:
                self.Dn[channel_name] = dn
                self.Ref[channel_name] = ref
            else:
                self.Dn[channel_name] = np.concatenate((self.Dn[channel_name], dn))
                self.Ref[channel_name] = np.concatenate((self.Ref[channel_name], ref))

            if channel_name not in self.cal_coeff1:
                self.cal_coeff1[channel_name] = [k[i, 0], k[i, 1], k[i, 2]]

        # 全局信息赋值 ############################
        # 对时间进行赋值合并
        v_ymd2seconds = np.vectorize(fy3_ymd2seconds)
        t1 = v_ymd2seconds(ary_day, ary_time)

        time = np.full(self.shape, -999)
        for i in xrange(self.shape[0]):
            time[i, :] = t1[i / 10, 0]
        if not self.Time:
            self.Time = time
        else:
            self.Time = np.concatenate((self.Time, time))

        # sv, bb
        for i in xrange(self.Band):
            channel_name = 'CH_%02d' % (i + 1)
            sv = np.full(self.shape, 32767)
            bb = np.full(self.shape, 32767)
            for j in xrange(self.shape[0]):
                sv[j, :] = ary_sv[i][j / 10]
                bb[j, :] = ary_bb[i][j / 10]
            if channel_name not in self.SV:
                self.SV[channel_name] = sv
                self.BB[channel_name] = bb
            else:
                self.SV[channel_name] = np.concatenate((self.SV[channel_name], sv))
                self.BB[channel_name] = np.concatenate((self.BB[channel_name], bb))

        # 系数先不合并，暂时未用，数据格式无法统一了
        self.VIS_Coeff = ary_vis_coeff

    @staticmethod
    def _create_dataset(name, data, dtype, hdf5, compression='gzip', compression_opts=5,
                        shuffle=True):
        dataset = hdf5.create_dataset(name, data=data, dtype=dtype,
                                      compression=compression,
                                      compression_opts=compression_opts,
                                      shuffle=shuffle)
        return dataset

    def write(self, out_file):
        """
        :return:
        """
        # 创建生成输出目录
        pb_io.make_sure_path_exists(os.path.dirname(out_file))

        # 将现在的第5通道数据复制一份到20通道 2018/07/27
        self.Ref['CH_20'] = self.Ref['CH_05']
        self.SV['CH_20'] = self.SV['CH_05']
        self.cal_coeff1['CH_20'] = self.cal_coeff1['CH_05']
        self.BB['CH_20'] = self.BB['CH_05']

        # 写入数据
        with h5py.File(out_file, 'w') as hdf5:
            for i in xrange(0, 20):
                channel_name = 'CH_{:02}'.format(i + 1)

                name = '{}/Ref'.format(channel_name)
                data = self.Ref[channel_name].astype('u2')
                dtype = 'u2'
                self._create_dataset(name, data, dtype, hdf5)

                name = '{}/SV'.format(channel_name)
                data = self.SV[channel_name].astype('u2')
                dtype = 'u2'
                self._create_dataset(name, data, dtype, hdf5)

                name = '{}/CalCoeff'.format(channel_name)
                data = self.cal_coeff1[channel_name]
                dtype = 'f4'
                self._create_dataset(name, data, dtype, hdf5)

                name = '{}/BB'.format(channel_name)
                data = self.BB[channel_name]
                dtype = 'f4'
                self._create_dataset(name, data, dtype, hdf5)

            name = 'Height'
            dtype = 'i2'
            data = self.ary_height
            self._create_dataset(name, data, dtype, hdf5)

            name = 'LandCover'
            dtype = 'u1'
            data = self.ary_land_cover
            self._create_dataset(name, data, dtype, hdf5)
            name = 'LandSeaMask'
            dtype = 'u1'
            data = self.ary_land_sea_mask
            self._create_dataset(name, data, dtype, hdf5)

            name = 'Latitude'
            dtype = 'f4'
            data = self.ary_lat
            self._create_dataset(name, data, dtype, hdf5)
            name = 'Longitude'
            dtype = 'f4'
            data = self.ary_lon
            self._create_dataset(name, data, dtype, hdf5)

            name = 'SolarZenith'
            dtype = 'i2'
            data = self.ary_sunz
            self._create_dataset(name, data, dtype, hdf5)
            name = 'SolarAzimuth'
            dtype = 'i2'
            data = self.ary_suna
            self._create_dataset(name, data, dtype, hdf5)
            name = 'SensorZenith'
            dtype = 'i2'
            data = self.ary_satz
            self._create_dataset(name, data, dtype, hdf5)
            name = 'SensorAzimuth'
            dtype = 'i2'
            data = self.ary_satz
            self._create_dataset(name, data, dtype, hdf5)

            name = 'Times'
            dtype = 'i4'
            data = self.Time
            self._create_dataset(name, data, dtype, hdf5)

            # 复制文件属性
            with h5py.File(self.in_file, 'r') as hdf5_in_file:
                pb_io.copy_attrs_h5py(hdf5_in_file, hdf5)


class CalibrateFY3B(object):
    """
    使用矫正系数对 MERSI L1 的产品进行定标预处理
    """

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

        self._get_ymd()
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

    def obc_sv_extract_fy3b(self, probe=None, probe_count=None, slide_step=None):
        """
        精提取 OBC 文件的 SV 值
        包括 250m reflective bands 和 1000m reflective bands

        # SV 提取(OBC文件)
        1: 250m 的前 4 个通道，先把 SV 8000*24 使用 40 个探元转换为 200 * 24 (选择 40 个探元中某个探元号的数据)，每个通道，可配置参数取某一个探元号
        2: 1000m 的 15 个通道，先把 SV 2000*6 使用 10 个探元 变为 200 * 6 (选择 10 个探元中某个探元号的数据)，每个通道，可配置参数取某一个探元号
        3: 然后按照 10 行滑动 从 200 * 6 变成 200 * 1
        4: 然后 10 行用 1 个 SV 均值，从 200 * 1 变成 2000 * 1
        5: 总共 19 * 2000 * 1(sv_2000)
        :param slide_step:  (list) 不同通道滑动的时候使用的步长
        :param probe_count: （list） 不同通道的探元总数
        :param probe: (list) 不同通道使用的探元号
        :return:
        """
        if self.error:
            return
        if not isinstance(probe, list) or len(probe) != 19:
            print "probe arg in yaml file has error."
            self.error = True
            return
        # 获取数据集
        setnames_obc = ['SV_1km', 'SV_250m_REFL']
        datasets_obc = pb_io.read_dataset_hdf5(self.obc_1000m, setnames_obc)

        # 提取 SV
        for i in xrange(19):
            if i < 4:
                dataset = datasets_obc['SV_250m_REFL'][i]
            else:
                dataset = datasets_obc['SV_1km'][i - 4]

            count = probe_count[i]  # 探元总数
            probe_id = probe[i]  # 探元 id
            step = slide_step[i]  # 滑动的步长

            sv_dataset = dataset_extract(dataset, count, probe_id, step)
            self.sv_extract_obc.append(sv_dataset)

    def calibrate(self):
        """
        进行预处理
        2013年之前
        1: ev_dn_l1 = ev_dn_l1 * slope_ev + intercept_ev 【原 L1 文件的 DN 值】
        4: slope = dsl ** 2 * k2 + dsl * k1 + k0  【# k0, k1, k2 是新的】
        5: ref_new = ((ev_dn_l1 - sv_dn_obc) * slope) * 100  【# 四舍五入取整】

        2013年之后
        1: ev_ref_l1 = ev_ref_l1 * slope_ev + intercept_ev
        2: slope_old = dsl**2 * k2_old + dsl * k1_old + k0_old
        【# k0, k1, k2 是原文件 RSB_Cal_Cor_Coeff 储存的】
        3: dn_new = ev_ref_l1 / slope_old + sv_dn_l1
        4: slope_new = dsl**2 * k2_new + dsl * k1_new + k0_new
        【# k0, k1, k2 是新给的】
        5: ref_new = ((dn_new - sv_dn_obc) * slope_new) * 100 【# 四舍五入取整】
        """
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

                sv_dn_obc = self.sv_extract_obc[i]

                k0, k1, k2 = self.coeff[i]

                # 除去 sv 数据中 0 对应的 dn 值
                idx = np.where(sv_dn_obc == 0)
                ev_dn_l1[idx, :] = 0

                # 除去有效范围外的 dn 值
                ev_dn_l1 = np.ma.masked_less_equal(ev_dn_l1, 0)
                ev_dn_l1 = np.ma.masked_greater(ev_dn_l1, 4095)

                # 进行计算
                ev_dn_l1 = ev_dn_l1 * ev_slope + ev_intercept
                slope = (self.dsl ** 2) * k2 + self.dsl * k1 + k0
                dn_new = ev_dn_l1 - sv_dn_obc
                ref_new = dn_new * slope * 100

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

                sv_dn_obc = self.sv_extract_obc[i]

                k0_new, k1_new, k2_new = self.coeff[i]
                k0_old, k1_old, k2_old = coeff_old[i]

                # 除去 sv 数据中 0 对应的 dn 值
                idx = np.where(sv_dn_obc == 0)
                ev_ref_l1[idx, :] = 0

                # 除去有效范围外的 dn 值
                ev_ref_l1 = np.ma.masked_less_equal(ev_ref_l1, 0)
                ev_ref_l1 = np.ma.masked_greater(ev_ref_l1, 10000)

                # 进行计算
                ev_ref_l1 = ev_ref_l1 * ev_slope + ev_intercept
                slope_old = (self.dsl ** 2) * k2_old + self.dsl * k1_old + k0_old
                dn_new = ev_ref_l1 / slope_old + sv_dn_l1
                slope_new = (self.dsl ** 2) * k2_new + self.dsl * k1_new + k0_new
                dn_new = dn_new - sv_dn_obc
                ref_new = dn_new * slope_new * 100

                # 除去有效范围外的 dn 值
                ref_new = np.ma.masked_less_equal(ref_new, 0)
                ref_new = ref_new.filled(0)
                ref_new = ref_new.astype(np.uint16)

                if i < 4:
                    self.ev_250m_ref.append(ref_new)
                else:
                    self.ev_1000m_ref.append(ref_new)

    @staticmethod
    def _create_dataset(name, data, dtype, hdf5, compression='gzip', compression_opts=5,
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
                            data = self.sv_extract_obc[i]
                        elif i == 4:
                            data = obc.get('SV_250m_EMIS')[:]
                        else:
                            i = i - 1
                            data = self.sv_extract_obc[i]
                        dtype = 'u2'
                        self._create_dataset(name, data, dtype, hdf5)

                        name = '{}/CalCoeff'.format(channel_name)
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


def dataset_extract(dataset, probe_count, probe_id, slide_step=10):
    """
    提取数据集的数据, 将 x * x 的数据提取为 y * 1
    :param slide_step: 滑动步长
    :param dataset: 二维数据集
    :param probe_count: (int) 探元总数量
    :param probe_id: (int) 此通道对应的探元 id
    :return:
    """
    # 筛选窗区内探元号对应的行
    dataset_ext = pb_calculate.extract_lines(dataset, probe_count, probe_id)
    # 滑动计算 avg 和 std
    avg_std_list = pb_calculate.rolling_calculate_avg_std(dataset_ext, slide_step)
    # 过滤标准差范围内的有效值
    dataset_valid = pb_calculate.filter_valid_value(dataset_ext, avg_std_list, 2)
    # 计算均值
    dataset_avg = pb_calculate.calculate_avg(dataset_valid)
    dataset_avg = np.array(dataset_avg).reshape(len(dataset_avg), 1)
    # 将行数扩大 10 倍
    dataset_avg = pb_calculate.expand_dataset_line(dataset_avg, 10)
    # 对浮点数据数据进行四舍五入
    dataset_new = np.rint(dataset_avg)
    return dataset_new


def get_files_by_ymd(dir_path, ymd_start, ymd_end, ext=None, pattern_ymd=None):
    """
    :param dir_path: 文件夹
    :param ymd_start: 开始时间
    :param ymd_end: 结束时间
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
                if os.path.splitext(file_name)[1].lower() != ext:
                    continue
            re_result = re.match(pattern, file_name)
            if re_result is not None:
                ymd_file = re_result.groups()[0]
            else:
                continue
            if int(ymd_start) <= int(ymd_file) <= int(ymd_end):
                files_found.append(os.path.join(root, file_name))
    return files_found


if __name__ == '__main__':
    # file_name = 'FY3B_MERSI_GBAL_L1_20170101_0045_1000M_MS.HDF'
    # Time = np.full((2000, 2048), -999.)
    # nameClass = pb_name.nameClassManager()
    # info = nameClass.getInstance(file_name)
    # secs = int((info.dt_s - datetime(1970, 1, 1, 0, 0, 0)).total_seconds())
    # Time[:] = secs
    # print Time
    # if not self.Time:
    #     self.Time = Time
    # else:
    #     self.Time = np.concatenate((self.Time, Time))

    # in_file = r'E:\projects\oc_data\FY3D_MERSI_GBAL_L1_20180101_0000_1000M_MS.HDF'
    # geo_file = r'E:\projects\oc_data\FY3D_MERSI_GBAL_L1_20180101_0000_GEO1K_MS.HDF'
    # out_file = r'E:\projects\oc_data\FY3D_MERSI_GBAL_L1_20180101_0000_1000M_MS_Calibration.HDF'
    #
    # calibration = CalibrateFY3D()
    # calibration.calibrate(in_file, geo_file)
    # calibration.write(out_file)

    pass

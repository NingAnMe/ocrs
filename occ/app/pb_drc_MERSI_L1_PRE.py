# coding: utf-8

'''
Created on 2017年9月7日

@author: wangpeng
'''
# 获取类py文件所在的目录

from datetime import datetime
import os
import sys
import time

import h5py

from PB import pb_sat
from PB.pb_time import fy3_ymd2seconds
import numpy as np


MainPath, MainFile = os.path.split(os.path.realpath(__file__))


class CLASS_MERSI_L1_PRE():

    '''
    1km的mersi2数据类
    '''

    def __init__(self):

        # 定标使用
        self.sat = 'FY3B'
        self.sensor = 'MERSI'
        self.res = 1000
        self.Band = 20
        self.obrit_direction = []
        self.obrit_num = []
        self.Dn = {}
        self.Ref = {}
        self.Rad = {}
        self.Tbb = {}
        self.satAzimuth = []
        self.satZenith = []
        self.sunAzimuth = []
        self.sunZenith = []
        self.Lons = []
        self.Lats = []
        self.Time = []
        self.SV = {}
        self.BB = {}
        self.LandSeaMask = []
        self.LandCover = []
        self.old_coeff = {}
        self.new_coeff = {}

    def get_coeff(self, coeff_file):
        if not os.path.isfile(coeff_file):
            print "File is not exist: {}".format(coeff_file)
            return
        try:
            new_coeff = np.loadtxt(coeff_file)
        except Exception as why:
            print why

        for i in xrange(new_coeff.shape[0]):
            if i < 4:
                band = 'CH_{:02d}'.format(i + 1)
            else:
                band = 'CH_{:02d}'.format(i + 2)
            self.new_coeff[band] = new_coeff[i]
        self.new_coeff['CH_05'] = np.array([1., 0, 0])

        for key in sorted(self.new_coeff.keys()):
            print key, self.new_coeff[key]

    def Load(self, L1File):
        # 读取L1文件 FY3B MERSI
        print (u'读取 L1 %s' % L1File)
        try:
            h5File_R = h5py.File(L1File, 'r')
            self.dsl = h5File_R.attrs.get('dsl')
            ary_lon = h5File_R.get('/Longitude')[:]
            ary_lat = h5File_R.get('/Latitude')[:]
            ary_times = h5File_R.get('/Times')[:]
            ary_satz = h5File_R.get('/SensorZenith')[:]
            ary_sata = h5File_R.get('/SensorAzimuth')[:]
            ary_sunz = h5File_R.get('/SolarZenith')[:]
            ary_suna = h5File_R.get('/SolarAzimuth')[:]
            ary_LandCover = h5File_R.get('/LandCover')[:]
            ary_LandSeaMask = h5File_R.get('/LandSeaMask')[:]

            for key in h5File_R.keys():
                pre_rootgrp = h5File_R.get(key)  # 获取根下名字
                if type(pre_rootgrp).__name__ == "Group":
                    ary_ref = h5File_R.get('/%s/Ref' % key)[:]
                    self.Ref[key] = ary_ref / 10000.
                    ary_coeff = h5File_R.get('/%s/CalCoeff' % key)[:]
                    self.old_coeff[key] = ary_coeff

        except Exception as e:
            print str(e)
            return
        finally:
            h5File_R.close()

        # 数据大小 使用经度维度 ###############

        for key in self.Ref.keys():
            idx = np.where(np.isclose(self.Ref[key], 0.))
            self.Ref[key][idx] = np.nan

        dshape = ary_lon.shape

        self.Time = ary_times

        # 土地覆盖
        ary_LandCover_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_LandCover >= 0, ary_LandCover <= 254)
        ary_LandCover_idx[condition] = ary_LandCover[condition]

        if self.LandCover == []:
            self.LandCover = ary_LandCover_idx
        else:
            self.LandCover = np.concatenate(
                (self.LandCover, ary_LandCover_idx))

        # 海陆掩码
        ary_LandSeaMask_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_LandSeaMask >= 0, ary_LandSeaMask <= 7)
        ary_LandSeaMask_idx[condition] = ary_LandSeaMask[condition]

        if self.LandSeaMask == []:
            self.LandSeaMask = ary_LandSeaMask_idx
        else:
            self.LandSeaMask = np.concatenate(
                (self.LandSeaMask, ary_LandSeaMask_idx))

        # 经纬度
        ary_lon_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_lon > -180., ary_lon < 180.)
        ary_lon_idx[condition] = ary_lon[condition]
        if self.Lons == []:
            self.Lons = ary_lon_idx
        else:
            self.Lons = np.concatenate((self.Lons, ary_lon_idx))

        ary_lat_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_lat > -90., ary_lat < 90.)
        ary_lat_idx[condition] = ary_lat[condition]
        if self.Lats == []:
            self.Lats = ary_lat_idx
        else:
            self.Lats = np.concatenate((self.Lats, ary_lat_idx))

        # 卫星方位角 天顶角
        ary_sata_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_sata > -18000, ary_sata < 18000)
        ary_sata_idx[condition] = ary_sata[condition]

        if self.satAzimuth == []:
            self.satAzimuth = ary_sata_idx / 100.
        else:
            self.satAzimuth = np.concatenate(
                (self.satAzimuth, ary_sata_idx / 100.))

        ary_satz_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_satz > 0, ary_satz < 18000)
        ary_satz_idx[condition] = ary_satz[condition]
        if self.satZenith == []:
            self.satZenith = ary_satz_idx / 100.
        else:
            self.satZenith = np.concatenate(
                (self.satZenith, ary_satz_idx / 100.))

        # 太阳方位角 天顶角
        ary_suna_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_suna > -18000, ary_suna < 18000)
        ary_suna_idx[condition] = ary_suna[condition]

        if self.sunAzimuth == []:
            self.sunAzimuth = ary_suna_idx / 100.
        else:
            self.sunAzimuth = np.concatenate(
                (self.sunAzimuth, ary_suna_idx / 100.))

        ary_sunz_idx = np.full(dshape, np.nan)
        condition = np.logical_and(ary_sunz > 0, ary_sunz < 18000)
        ary_sunz_idx[condition] = ary_sunz[condition]

        if self.sunZenith == []:
            self.sunZenith = ary_sunz_idx / 100.
        else:
            self.sunZenith = np.concatenate(
                (self.sunZenith, ary_sunz_idx / 100.))

    def get_new_ref(self):
        dsl = self.dsl
        for key in self.Ref.keys():
            k0 = self.old_coeff[key][0]
            k1 = self.old_coeff[key][1]
            k2 = self.old_coeff[key][2]
#             print key, 'old k', k2, k1, k0
            k22 = self.new_coeff[key][2]
            k11 = self.new_coeff[key][1]
            k00 = self.new_coeff[key][0]
            old_slope = (dsl**2) * k2 + dsl * k1 + k0
            new_slope = (dsl**2) * k22 + dsl * k11 + k00
            dn = self.Ref[key] / old_slope
            new_ref = dn * new_slope
            print 'ref', self.Ref[key][1000, 1000]
            self.Ref[key] = new_ref
            print '################### %s' % key

            print 'dn', dn[1000, 1000]
            print 'new', new_ref[1000, 1000]

    def sun_earth(self, ymd):
        stime = datetime.strptime(ymd, '%Y%m%d')
        jjj = int(stime.strftime('%j'))
        EarthSunDist = (1.00014 - 0.01671 * np.cos(1.0 * 2 * np.pi * (0.9856002831 * jjj - 3.4532868) /
                                                   360.0) - 0.00014 * np.cos(2.0 * 2 * np.pi * (0.9856002831 * jjj - 3.4532868) / 360.0))

        f_jday = np.power(1.0 / EarthSunDist, 2)

        for key in self.Ref.keys():
            self.Ref[key] = (self.Ref[key] / f_jday)


if __name__ == '__main__':

    #     1884
    #dn * ((dsl ** 2) * k2 + dsl * k1 + k0) * 100.
    slope_old = (1884**2) * 2.9E-10 + 1884 * -3.468E-7 + 0.02871
#     print slope_old
#     print 2701 / slope_old
#     sys.exit(1)
    L1File = 'D:/data/MERSI_OCC/FY3B_MERSI_GBAL_L1_20130101_0020_1000M_MS.HDF'
    coeff_file = 'D:/data/MERSI_OCC/2013.txt'
    mersi = CLASS_MERSI_L1_PRE()
    mersi.Load(L1File)
    mersi.get_coeff(coeff_file)
    mersi.get_new_ref()

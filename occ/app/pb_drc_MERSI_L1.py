# coding: utf-8

'''
Created on 2017年9月7日

@author: wangpeng
'''
# 获取类py文件所在的目录

import os
import sys
import time

import h5py

from PB import pb_sat
from PB.pb_time import fy3_ymd2seconds
import numpy as np


MainPath, MainFile = os.path.split(os.path.realpath(__file__))


class CLASS_MERSI_L1():

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

    def Load(self, L1File):
        # 读取L1文件 FY3B MERSI
        try:
            h5File_R = h5py.File(L1File, 'r')
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
                    print key
                    ary_ref = h5File_R.get('/%s/Ref' % key)[:]
                    print ary_ref

        except Exception as e:
            print str(e)
            return
        finally:
            h5File_R.close()

        # 数据大小 使用经度维度 ###############
        dshape = ary_lon.shape

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
        condition = np.logical_and(ary_sata > 0, ary_sata < 36000)
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
        condition = np.logical_and(ary_suna > 0, ary_suna < 36000)
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

if __name__ == '__main__':
    L1File = 'D:/data/FY3B_MERSI/FY3B_MERSI_GBAL_L1_20130101_0005_1000M_MS.HDF'
    mersi = CLASS_MERSI_L1()
    mersi.Load(L1File)
    pass

# coding: utf-8
# 引用自编库
'''
Description:  静止和极轨通用匹配程序
Author:       wangpeng
Date:         2017-08-10
version:      1.0.1_beat
Input:        yaml格式配置文件
Output:       hdf5格式匹配文件  (^_^)
'''
# 配置文件信息，设置为全局

from datetime import datetime
import os
import sys

import h5py
import yaml

from DP.dp_2d import rolling_2d_window_pro
from DP.dp_prj import prj_core, fill_points_2d
from DV import dv_plt
from DV.dv_map import dv_map
from PB.pb_sat import sun_glint_cal
from app.pb_drc_MERSI_L1 import CLASS_MERSI_L1
from app.pb_drc_MODIS_L1 import CLASS_MODIS_L1
import numpy as np


MainPath, MainFile = os.path.split(os.path.realpath(__file__))


class ReadYaml():

    def __init__(self, inFile):
        """
        读取yaml格式配置文件
        """
        if not os.path.isfile(inFile):
            print 'Not Found %s' % inFile
            sys.exit(-1)

        with open(inFile, 'r') as stream:
            cfg = yaml.load(stream)
        self.sat1 = cfg['INFO']['sat1']
        self.sensor1 = cfg['INFO']['sensor1']
        self.sat2 = cfg['INFO']['sat2']
        self.sensor2 = cfg['INFO']['sensor2']
        self.ymd = cfg['INFO']['ymd']
#         self.hms = cfg['INFO']['hms']

        self.ifile1 = cfg['PATH']['ipath1']
        self.ifile2 = cfg['PATH']['ipath2']
        self.ofile = cfg['PATH']['opath']

        self.cmd = cfg['PROJ']['cmd']
        self.col = cfg['PROJ']['col']
        self.row = cfg['PROJ']['row']
        self.res = cfg['PROJ']['res']


class ReadModeYaml():
    """
        读取yaml格式配置文件,解析匹配的传感器对的默认配置参数
    """

    def __init__(self, inFile):

        if not os.path.isfile(inFile):
            print 'Not Found %s' % inFile
            sys.exit(-1)

        with open(inFile, 'r') as stream:
            cfg = yaml.load(stream)
        self.sensor1 = cfg['sensor1']
        self.sensor2 = cfg['sensor2']
        self.chan1 = cfg['chan1']
        self.chan2 = cfg['chan2']
        self.rewrite = cfg['rewrite']

        self.FovWind1 = tuple(cfg['FovWind1'])
        self.EnvWind1 = tuple(cfg['EnvWind1'])
        self.FovWind2 = tuple(cfg['FovWind2'])
        self.EnvWind2 = tuple(cfg['EnvWind2'])

        self.solglint_min = cfg['solglint_min']
        self.solzenith_max = cfg['solzenith_max']
        self.satzenith_max = cfg['satzenith_max']
        self.timedif_max = cfg['timedif_max']
        self.angledif_max = cfg['angledif_max']
        self.distdif_max = cfg['distdif_max']

        # 将通道阈值放入字典
        self.CH_threshold = {}
        for ch in self.chan1:
            if ch not in self.CH_threshold.keys():
                self.CH_threshold[ch] = {}
            for threshold in cfg[ch]:
                self.CH_threshold[ch][threshold] = cfg[ch][threshold]


class COLLOC_COMM(object):
    """
    交叉匹配的公共类，首先初始化所有参数信息
    """

    def __init__(self, row, col, BandLst):

        # 默认填充值 和 数据类型
        self.row = row
        self.col = col
        self.FillValue = -999.
        self.dtype = 'f4'
        self.BandLst = BandLst

        # 投影后的全局变量信息
        self.S1_Time = np.full((row, col), self.FillValue, self.dtype)
        self.S1_Lon = np.full((row, col), self.FillValue, self.dtype)
        self.S1_Lat = np.full((row, col), self.FillValue, self.dtype)
        self.S1_SatA = np.full((row, col), self.FillValue, self.dtype)
        self.S1_SatZ = np.full((row, col), self.FillValue, self.dtype)
        self.S1_SoA = np.full((row, col), self.FillValue, self.dtype)
        self.S1_SoZ = np.full((row, col), self.FillValue, self.dtype)

        self.S2_Time = np.full((row, col), self.FillValue, self.dtype)
        self.S2_Lon = np.full((row, col), self.FillValue, self.dtype)
        self.S2_Lat = np.full((row, col), self.FillValue, self.dtype)
        self.S2_SatA = np.full((row, col), self.FillValue, self.dtype)
        self.S2_SatZ = np.full((row, col), self.FillValue, self.dtype)
        self.S2_SoA = np.full((row, col), self.FillValue, self.dtype)
        self.S2_SoZ = np.full((row, col), self.FillValue, self.dtype)

        # 粗匹配掩码记录表
        self.MaskRough = np.full((row, col), 0, 'i1')
        self.PubIdx = np.full((row, col), 0, 'i1')

        # 精匹配掩码记录表，按照通道存放
        self.MaskFine = {}

        # 初始化字典内的存放每个通道的数据空间
        for band in BandLst:
            self.MaskFine[band] = np.full((row, col), 0, 'i1')

    def save_rough_data(self, P1, P2, D1, D2, modeCfg):
        """
        第一轮匹配，根据查找表进行数据的mean和std计算，并且对全局物理量复制（角度，经纬度，时间等）
        """

        # 公共的投影区域位置信息

        condition = np.logical_and(P1.lut_i > 0, P2.lut_i > 0)
        idx = np.where(condition)
        print u'FY LEO 公共区域匹配点个数 %d' % len(idx[0])
        # 粗匹配点没有则返回
        if len(idx[0]) == 0:
            return
        # 记录粗匹配点
        self.PubIdx[idx] = 1

        # 投影后网格，公共区域的投影后数据的行列
        p_i = idx[0]
        p_j = idx[1]

        # 投影后网格，公共区域的投影后 传感器1 和 传感器2 数据的行列
        i1 = P1.lut_i[idx]
        j1 = P1.lut_j[idx]
        i2 = P2.lut_i[idx]
        j2 = P2.lut_j[idx]

        print u'对公共区域位置进行数据赋值......'
        # 11111111 保存传感器1,2 的投影公共数据信息
        self.S1_Time[idx] = D1.Time[i1, j1]
        self.S1_Lon[idx] = D1.Lons[i1, j1]
        self.S1_Lat[idx] = D1.Lats[i1, j1]
        self.S1_SatA[idx] = D1.satAzimuth[i1, j1]
        self.S1_SatZ[idx] = D1.satZenith[i1, j1]
        self.S1_SoA[idx] = D1.sunAzimuth[i1, j1]
        self.S1_SoZ[idx] = D1.sunZenith[i1, j1]

        self.S2_Time[idx] = D2.Time[i2, j2]
        self.S2_Lon[idx] = D2.Lons[i2, j2]
        self.S2_Lat[idx] = D2.Lats[i2, j2]
        self.S2_SatA[idx] = D2.satAzimuth[i2, j2]
        self.S2_SatZ[idx] = D2.satZenith[i2, j2]
        self.S2_SoA[idx] = D2.sunAzimuth[i2, j2]
        self.S2_SoZ[idx] = D2.sunZenith[i2, j2]

        # 保存辅助信息
        self.__init_class_array_geo(
            D1, 'LandCover', 'S1_LandCover', i1, j1, p_i, p_j)
        self.__init_class_array_geo(
            D1, 'LandSeaMask', 'S1_LandSeaMask', i1, j1, p_i, p_j)

        self.__init_class_array_geo(
            D2, 'LandSeaMask', 'S2_LandSeaMask', i2, j2, p_i, p_j)
        self.__init_class_array_geo(
            D2, 'LandSeaMask', 'S2_LandSeaMask', i2, j2, p_i, p_j)

        self.__init_class_dict_geo(
            D1, 'SV', 'S1_SV', modeCfg, i1, j1, p_i, p_j)
        self.__init_class_dict_geo(
            D1, 'BB', 'S1_BB', modeCfg, i1, j1, p_i, p_j)
        self.__init_class_dict_geo(
            D1, 'cal_coeff2', 'S1_IrCalSlope', modeCfg, i1, j1, p_i, p_j)
        self.__init_class_dict_geo(
            D2, 'SV', 'S2_SV', modeCfg, i2, j2, p_i, p_j)
        self.__init_class_dict_geo(
            D2, 'BB', 'S2_BB', modeCfg, i2, j2, p_i, p_j)

        # 关键物理量的均值和std计算
        self.__init_class_dict_dn(D1, 'Ref', 'S1', modeCfg, i1, j1, p_i, p_j)
        self.__init_class_dict_dn(D2, 'Ref', 'S2', modeCfg, i2, j2, p_i, p_j)

    def __init_class_dict_dn(self, idata, name, sensor, modeCfg, i, j, x, y):

        member1 = '%s_Fov%sMean' % (sensor, name)
        member2 = '%s_Fov%sStd' % (sensor, name)
        member3 = '%s_Env%sMean' % (sensor, name)
        member4 = '%s_Env%sStd' % (sensor, name)

        if hasattr(idata, name) and not hasattr(self, member1):
            self.__dict__[member1] = {}
            self.__dict__[member2] = {}
            self.__dict__[member3] = {}
            self.__dict__[member4] = {}

            for band1 in modeCfg.chan1:
                index = modeCfg.chan1.index(band1)
                band2 = modeCfg.chan2[index]
                if 'S1' in sensor:
                    band = band1
                elif 'S2' in sensor:
                    band = band2

                if band in eval('idata.%s.keys()' % name):
                    self.__dict__[member1][band1] = np.full(
                        (self.row, self.col), self.FillValue, self.dtype)
                    self.__dict__[member2][band1] = np.full(
                        (self.row, self.col), self.FillValue, self.dtype)
                    self.__dict__[member3][band1] = np.full(
                        (self.row, self.col), self.FillValue, self.dtype)
                    self.__dict__[member4][band1] = np.full(
                        (self.row, self.col), self.FillValue, self.dtype)

        if hasattr(self, member1):
            for band1 in modeCfg.chan1:
                index = modeCfg.chan1.index(band1)
                band2 = modeCfg.chan2[index]
                if 'S1' in sensor:
                    band = band1
                    FovWind = modeCfg.FovWind1
                    EnvWind = modeCfg.EnvWind1
                elif 'S2' in sensor:
                    band = band2
                    FovWind = modeCfg.FovWind2
                    EnvWind = modeCfg.EnvWind2
                if band in eval('idata.%s.keys()' % name):
                    # sat1 Fov和Env dn的mean和std
                    data = eval('idata.%s["%s"]' % (name, band))
                    # 计算各个通道的投影后数据位置对应原始数据位置点的指定范围的均值和std
                    mean, std, pi, pj = rolling_2d_window_pro(
                        data, FovWind, i, j, x, y)
                    self.__dict__[member1][band1][pi, pj] = mean
                    self.__dict__[member2][band1][pi, pj] = std
                    mean, std, pi, pj = rolling_2d_window_pro(
                        data, EnvWind, i, j, x, y)
                    self.__dict__[member3][band1][pi, pj] = mean
                    self.__dict__[member4][band1][pi, pj] = std

    def __init_class_dict_geo(self, idata, name1, name2, modeCfg, i, j, x, y):
        '''
        直接使用查找表赋值，不需要计算均值std 和 窗区
        '''

        if hasattr(idata, name1) and not hasattr(self, name2):
            self.__dict__[name2] = {}

            for band1 in modeCfg.chan1:
                index = modeCfg.chan1.index(band1)
                band2 = modeCfg.chan2[index]
                if 'S1' in name2:
                    band = band1
                elif 'S2' in name2:
                    band = band2

                if band in eval('idata.%s.keys()' % name1):
                    self.__dict__[name2][band1] = np.full(
                        (self.row, self.col), self.FillValue, self.dtype)

        if hasattr(self, name2):
            for band1 in modeCfg.chan1:
                index = modeCfg.chan1.index(band1)
                band2 = modeCfg.chan2[index]
                if 'S1' in name2:
                    band = band1
                elif 'S2' in name2:
                    band = band2
                if band in eval('idata.%s.keys()' % name1):
                    data = eval('idata.%s["%s"]' % (name1, band))
                    self.__dict__[name2][band1][x, y] = data[i, j]

    def __init_class_array_geo(self, idata, name1, name2, i, j, x, y):

        if hasattr(idata, name1) and not hasattr(self, name2):
            self.__dict__[name2] = np.full((self.row, self.col), -999, 'i2')
        if hasattr(self, name2):
            data = eval('idata.%s' % name1)
            self.__dict__[name2][x, y] = data[i, j]

    def save_fine_data(self, modeCfg):
        """
        第二轮匹配，根据各通道的的mean和std计以为，角度和距离等进行精细化过滤
        """

        # 最终的公共匹配点数量
        idx = np.where(self.PubIdx > 0)
        if len(idx[0]) == 0:
            return
        print u'所有粗匹配点数目 ', len(idx[0])

        # 掩码清零
        self.MaskRough[:] = 0

        # 计算共同区域的距离差 #########
        disDiff = np.full_like(self.S1_Time, '-1', dtype='i2')
        a = np.power(self.S2_Lon[idx] - self.S1_Lon[idx], 2)
        b = np.power(self.S2_Lat[idx] - self.S1_Lat[idx], 2)
        disDiff[idx] = np.sqrt(a + b) * 100.

        idx_Rough = np.logical_and(disDiff < modeCfg.distdif_max, disDiff >= 0)
        idx1 = np.where(idx_Rough)
        print u'1. 距离过滤后剩余点 ', len(idx1[0])

        timeDiff = np.abs(self.S1_Time - self.S2_Time)

        idx_Rough = np.logical_and(idx_Rough, timeDiff <= modeCfg.timedif_max)
        idx1 = np.where(idx_Rough)
        print u'2. 时间过滤后剩余点 ', len(idx1[0])
        # 过滤太阳天顶角 ###############
        idx_Rough = np.logical_and(
            idx_Rough, self.S1_SoZ <= modeCfg.solzenith_max)
        idx_Rough = np.logical_and(
            idx_Rough, self.S2_SoZ <= modeCfg.solzenith_max)
        idx1 = np.where(idx_Rough)
        print u'3. 太阳天顶角过滤后剩余点 ', len(idx1[0])

        # 计算耀斑角 ###############
        glint1 = np.full_like(self.S1_SatZ, -999.)
        glint2 = np.full_like(self.S1_SatZ, -999.)

        glint1[idx] = sun_glint_cal(
            self.S1_SatA[idx], self.S1_SatZ[idx], self.S1_SoA[idx], self.S1_SoZ[idx])
        glint2[idx] = sun_glint_cal(
            self.S2_SatA[idx], self.S2_SatZ[idx], self.S2_SoA[idx], self.S2_SoZ[idx])

        idx_Rough = np.logical_and(idx_Rough, glint1 > modeCfg.solglint_min)
        idx_Rough = np.logical_and(idx_Rough, glint2 > modeCfg.solglint_min)
        print np.nanmin(glint1[idx]), np.nanmax(glint1[idx])
        print np.nanmin(glint2[idx]), np.nanmax(glint2[idx])
        idx1 = np.where(idx_Rough)
        print u'4. 太阳耀斑角过滤后剩余点 ', len(idx1[0])

        # 角度均匀性 #################
        SatZRaio = np.full_like(self.S1_Time, 9999)
        SatZ1 = np.cos(self.S1_SatZ[idx] * np.pi / 180.)
        SatZ2 = np.cos(self.S2_SatZ[idx] * np.pi / 180.)
        SatZRaio[idx] = np.abs(SatZ1 / SatZ2 - 1.)

        idx_Rough = np.logical_and(idx_Rough, SatZRaio <= modeCfg.angledif_max)
        idx1 = np.where(idx_Rough)
        print u'5. 卫星天顶角均匀性过滤后剩余点 ', len(idx1[0])

        idx_Rough = np.logical_and(
            idx_Rough, self.S1_SatZ <= modeCfg.satzenith_max)
        idx1 = np.where(idx_Rough)
        print u'6. FY卫星观测角(天顶角)滤后剩余点 ', len(idx1[0])
        self.MaskRough[idx1] = 1

        for Band1 in modeCfg.chan1:
            # 掩码清零
            self.MaskFine[Band1][:] = 0

            th_vaue_max = modeCfg.CH_threshold[Band1]['value_max']
            th1 = modeCfg.CH_threshold[Band1]['angledif_max']
            th2 = modeCfg.CH_threshold[Band1]['homodif_fov_max']
            th3 = modeCfg.CH_threshold[Band1]['homodif_env_max']
            th4 = modeCfg.CH_threshold[Band1]['homodif_fov_env_max']

            flag = 0

            # 可见光通道
            if hasattr(self, 'S1_FovRefMean') and Band1 in self.S1_FovRefMean.keys():
                flag = 'vis'
                homoFov1 = np.abs(
                    self.S1_FovRefStd[Band1] / self.S1_FovRefMean[Band1])
                homoEnv1 = np.abs(
                    self.S1_EnvRefStd[Band1] / self.S1_EnvRefMean[Band1])
                homoFovEnv1 = np.abs(
                    self.S1_FovRefMean[Band1] / self.S1_EnvRefMean[Band1] - 1)
                homoValue1 = self.S1_FovRefMean[Band1]

                homoFov2 = np.abs(
                    self.S2_FovRefStd[Band1] / self.S2_FovRefMean[Band1])
                homoEnv2 = np.abs(
                    self.S2_EnvRefStd[Band1] / self.S2_EnvRefMean[Band1])
                homoFovEnv2 = np.abs(
                    self.S2_FovRefMean[Band1] / self.S2_EnvRefMean[Band1] - 1)
                homoValue2 = self.S2_FovRefMean[Band1]

            condition = np.logical_and(self.MaskRough > 0, True)
            condition = np.logical_and(SatZRaio < th1, condition)
            idx = np.where(condition)
            print u'%s %s 角度均匀性过滤后，精匹配点个数 %d' % (Band1, flag, len(idx[0]))

            condition = np.logical_and(homoValue1 < th_vaue_max, condition)
            condition = np.logical_and(homoValue1 > 0, condition)
            idx = np.where(condition)
            print u'%s %s 饱和值过滤后，精匹配点个数 %d' % (Band1, flag, len(idx[0]))

            condition = np.logical_and(homoFov1 < th2, condition)
            idx = np.where(condition)
            print u'%s %s 靶区过滤后，精匹配点个数 %d' % (Band1, flag, len(idx[0]))

            condition = np.logical_and(homoEnv1 < th3, condition)
            idx = np.where(condition)
            print u'%s %s 环境过滤后，精匹配点个数 %d' % (Band1, flag, len(idx[0]))

            condition = np.logical_and(homoFovEnv1 < th4, condition)
            idx = np.where(condition)
            print u'%s %s 靶区环境过滤后，精匹配点个数 %d' % (Band1, flag, len(idx[0]))

            # sat 2过滤

            condition = np.logical_and(homoValue2 > 0, condition)
            condition = np.logical_and(homoValue2 < th_vaue_max, condition)
            idx = np.where(condition)
            print u'%s %s 饱和值2过滤后，精匹配点个数 %d' % (Band1, flag, len(idx[0]))

            condition = np.logical_and(homoFov2 < th2, condition)
            idx = np.where(condition)
            print u'%s %s 靶区2过滤后，精匹配点个数 %d' % (Band1, flag, len(idx[0]))

            condition = np.logical_and(homoEnv2 < th3, condition)
            idx = np.where(condition)
            print u'%s %s 环境2过滤后，精匹配点个数 %d' % (Band1, flag, len(idx[0]))

            condition = np.logical_and(homoFovEnv2 < th4, condition)
            idx = np.where(condition)
            print u'%s %s 靶区环境2过滤后，精匹配点个数 %d' % (Band1, flag, len(idx[0]))

            self.MaskFine[Band1][idx] = 1

    def write_hdf5(self, ICFG, MCFG):

        print u'输出产品'
        for band in MCFG.chan1:
            idx = np.where(self.MaskFine[band] > 0)
            DCLC_nums = len(idx[0])
            if DCLC_nums > 0:
                break
        if DCLC_nums == 0:
            print('colloc point is zero')
            sys.exit(-1)

        # 创建文件夹
        MainPath, _ = os.path.split(ICFG.ofile)
        if not os.path.isdir(MainPath):
            os.makedirs(MainPath)

        # 创建hdf5文件
        h5File_W = h5py.File(ICFG.ofile, 'w')

        # 获取组外的数据集名称
        geo_list = []
        for member in self.__dict__.keys():
            dname = eval('self.%s' % member)
            if isinstance(dname, np.ndarray):
                if 'MaskRough' in member:
                    continue
                elif 'PubIdx' in member:
                    continue
                geo_list.append(member)

        # 根据组里的mask 把匹配上的数据选出来输出
        for member in self.__dict__.keys():
            dname = eval('self.%s' % member)
            if isinstance(dname, dict):
                # 注意，注意 ！！！ 记录光谱信息的字典要过滤掉，在重处理模式下这个没法更新，需要删除文件处理
                if 'MaskFine' in member:
                    continue
                for band in sorted(dname.keys()):
                    idx = np.where(self.MaskFine[band] > 0)
                    str_dname = '/' + band + '/' + member
                    dset = h5File_W.create_dataset(
                        str_dname, data=dname[band][idx], compression='gzip', compression_opts=5, shuffle=True)
        for band in MCFG.chan1:
            idx = np.where(self.MaskFine[band] > 0)
            for geo in geo_list:
                dname = eval('self.%s' % geo)
                str_dname = '/' + band + '/' + geo
                dset = h5File_W.create_dataset(
                    str_dname, data=dname[idx], compression='gzip', compression_opts=5, shuffle=True)
        h5File_W.close()

        print u'回归图'

        for Band in MCFG.chan1:
            idx = np.where(self.MaskFine[Band] > 0)
            x = self.S1_FovRefMean[Band][idx]
            y = self.S2_FovRefMean[Band][idx]
            if len(x) >= 2:
                value_min = value_max = None
                flag = 'Ref'
                print('ref', Band, len(x), np.min(x), np.max(x),
                      np.min(y), np.max(y))
                value_min = 0.
                value_max = 1.
                if value_min is not None and value_max is not None:
                    regression(x, y, value_min, value_max,
                               flag, ICFG, MCFG, Band)


def regression(x, y, value_min, value_max, flag, ICFG, MCFG, Band):

    # FY4分布
    MainPath, _ = os.path.split(ICFG.ofile)
    if not os.path.isdir(MainPath):
        os.makedirs(MainPath)

    meanbais = (np.mean(x - y) / np.mean(y)) * 100.

    p = dv_plt.dv_scatter(figsize=(7, 5))
    p.easyplot(x, y, None, None, marker='o', markersize=5)

    p.xlim_min = p.ylim_min = value_min
    p.xlim_max = p.ylim_max = value_max

    p.title = u'%s' % (ICFG.ymd)
    p.xlabel = u'%s %s %s' % (ICFG.sat1, ICFG.sensor1, flag)
    p.ylabel = u'%s %s %s' % (ICFG.sat2, ICFG.sensor2, flag)
    # 计算AB
    ab = np.polyfit(x, y, 1)
    p.regression(ab[0], ab[1], 'b')

    # 计算相关性
    p.show_leg = True
    r = np.corrcoef(x, y)
    rr = r[0][1] * r[0][1]
    nums = len(x)
    # 绘制散点
    strlist = [[r'$%0.4fx%+0.4f (R=%0.4f) $' % (ab[0], ab[1], rr),
                r'count:%d' % nums, r'%sMeanBias: %0.4f' % (flag, meanbais)]]
    p.annotate(strlist, 'left', 'r')
    ofile = os.path.join(MainPath, '%s+%s_%s+%s_%s_%s_%s.png' %
                         (ICFG.sat1, ICFG.sensor1, ICFG.sat2, ICFG.sensor2, ICFG.ymd, Band, flag))
    p.savefig(ofile, dpi=300)


def main(inYamlFile):
    T1 = datetime.now()

    # 01 ICFG = 输入配置文件类 ##########
    ICFG = ReadYaml(inYamlFile)

    # 02 MCFG = 阈值配置文件类
    modeFile = os.path.join(
        MainPath, 'cfg', '%s+%s_%s+%s.colloc' % (ICFG.sat1, ICFG.sensor1, ICFG.sat2, ICFG.sensor2))
    MCFG = ReadModeYaml(modeFile)
    if not MCFG.rewrite:
        print 'skip'
        return
    # DCLC = DATA DCLC 匹配结果类
    DCLC = COLLOC_COMM(ICFG.row, ICFG.col, MCFG.chan1)

    T2 = datetime.now()
    print 'read config:', (T2 - T1).total_seconds()

    # 判断是否重写
    if MCFG.rewrite:
        rewrite_mask = True
    else:
        if os.path.isfile(ICFG.ofile):
            rewrite_mask = False

    if rewrite_mask:
        T1 = datetime.now()
        # 03 解析 第一颗传感器的L1数据 ##########
        for inFile in ICFG.ifile1:
            D1 = CLASS_MERSI_L1()
            D1.Load(inFile)

            # 04 投影，简历查找表  ##########
            print ICFG.cmd
            P1 = prj_core(ICFG.cmd, ICFG.res, row=ICFG.row, col=ICFG.col)
            P1.create_lut(D1.Lons, D1.Lats)
            value = np.full_like(P1.lons, 20.)
            p_map = dv_map()
            p_map.easyplot(P1.lats, P1.lons,  value, markersize=20, marker='.')
            p_map.savefig('test_p1_1.png')
            # 05 解析 第二颗传感器的L1数据   ##########
            i = 0
            for inFile2 in ICFG.ifile2:
                i = i + 1
                D2 = CLASS_MODIS_L1()
                D2.Load(inFile2)

                # 06 投影，简历查找表  ##########
                P2 = prj_core(ICFG.cmd, ICFG.res, row=ICFG.row, col=ICFG.col)
                P2.create_lut(D2.Lons, D2.Lats)
                value = np.full_like(P2.lons, 40.)
                p_map = dv_map()
                p_map.easyplot(
                    P2.lats, P2.lons, value, markersize=20, marker='.')
                p_map.savefig('test_p2_%d.png' % i)
                # 07 粗匹配 ##########
                DCLC.save_rough_data(P1, P2, D1, D2, MCFG)

        T2 = datetime.now()
        print 'rough:', (T2 - T1).total_seconds()
        # 08 精匹配  和订正可见光通道的ref值 ##########
        T1 = datetime.now()
        DCLC.save_fine_data(MCFG)
        T2 = datetime.now()
        print 'colloc:', (T2 - T1).total_seconds()

        # 09 输出匹配结果 ##########
        T1 = datetime.now()
        DCLC.write_hdf5(ICFG, MCFG)
        T2 = datetime.now()
        print 'write:', (T2 - T1).total_seconds()

if __name__ == '__main__':

    # 获取python输入参数，进行处理
    args = sys.argv[1:]
    if len(args) == 1:  # 跟参数，则处理输入的时段数据
        inYamlFile = args[0]
    else:
        print 'input args error exit'
        sys.exit(-1)

    # 统计整体运行时间
    T_all_1 = datetime.now()
    main(inYamlFile)
    T_all_2 = datetime.now()
    print 'times:', (T_all_2 - T_all_1).total_seconds()

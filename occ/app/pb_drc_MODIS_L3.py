# coding: utf-8

'''
Created on 2017年9月7日

@author: wangpeng
'''

from datetime import datetime
import os
import re
import sys

from pyhdf.SD import SD, SDC
import h5py

from DV.dv_map import dv_map
from PB import pb_name, pb_space
from PB.pb_sat import sun_earth_dis_correction
import numpy as np


class CLASS_MODIS_L3():
    """
    水色
    """

    def __init__(self):

        self.Ref = {}
        self.Lons = []
        self.Lats = []
        self.Time = []

        self.dset_list1 = ['KD490_mean', 'CHL1_mean', 'POC_mean', 'ZSD_mean']
        self.dset_list2 = ['Kd490', 'CHL1', 'POC', 'ZSD']

    def Load(self, L1File):
        print (u'读取 L3 %s' % L1File)
        try:
            h5r = h5py.File(L1File, 'r')
            Lons = h5r.get('/lon')[:]
            Lats = h5r.get('/lat')[:]
            row = Lats.shape[0]
            col = Lons.shape[0]
#             aaa = np.repeat(Lons, row)
#             print aaa.shape
            self.Lons = (np.repeat(Lons, row)).reshape(col, row)
            self.Lats = (np.repeat(Lats, col)).reshape(row, col)
            self.Lons = np.rot90(self.Lons)
#             print self.Lons.shape
#             print self.Lons[0, 1]
#             print self.Lons[22, 1]
#             print self.Lats[0, 1]
#             print self.Lats[0, 22]
#             print self.Lats[1, -1]
#
#             print self.Lons.shape

            # 490
            dataset = h5r.get('/KD490_mean')
            dataset_value = dataset[:]
            condition = np.logical_and(dataset_value <= 0, True)
            value = dataset_value.astype(np.float32)
            value[condition] = np.nan
            self.Ref['Kd490'] = value

            # 处理定位文件，默认和L1同一目录  CHL1
            file2 = L1File.replace('KD490', 'CHL1')
            h5r = h5py.File(file2, 'r')
            dataset = h5r.get('/CHL1_mean')
            dataset_value = dataset[:]
            condition = np.logical_and(dataset_value <= 0, True)
            value = dataset_value.astype(np.float32)
            value[condition] = np.nan
            self.Ref['CHL1'] = value

            # 处理定位文件，默认和L1同一目录  POC
            file2 = L1File.replace('KD490', 'POC')
            h5r = h5py.File(file2, 'r')
            dataset = h5r.get('/POC_mean')
            dataset_value = dataset[:]
            condition = np.logical_and(dataset_value <= 0, True)
            value = dataset_value.astype(np.float32)
            value[condition] = np.nan
            self.Ref['POC'] = value

            # 处理定位文件，默认和L1同一目录  POC
            file2 = L1File.replace('KD490', 'ZSD')
            h5r = h5py.File(file2, 'r')
            dataset = h5r.get('/ZSD_mean')
            dataset_value = dataset[:]
            condition = np.logical_and(dataset_value <= 0, True)
            value = dataset_value.astype(np.float32)
            value[condition] = np.nan
            self.Ref['ZSD'] = value

        except Exception as e:
            print str(e)

if __name__ == '__main__':
    L1File = 'D:/data/modis_occ/A2013004003500.L2_LAC_OC.nc'
    L1File = 'D:/data/MERSI_OCC/L3m_20130108__GLOB_4_AV-MOD_KD490_DAY_00.nc'
    modis = CLASS_MODIS_L3()
    modis.Load(L1File)
    for key in modis.Ref.keys():
        print key, np.nanmin(modis.Ref[key]), np.nanmax(modis.Ref[key])

    for Band in modis.Ref.keys():
        p = dv_map()
        p.delat = 30
        p.delon = 30
        p.colorbar_fmt = '%0.2f'
        vmin = 0.
        vmax = 30.
        p.easyplot(
            modis.Lats, modis.Lons, modis.Ref[Band], vmin=vmin, vmax=vmax, markersize=5, marker='s')
        p.title = u'mersi'
        ofile = os.path.join('D:/data/MERSI_OCC/', 'MODIS_%s.png' % Band)
        p.savefig(ofile)

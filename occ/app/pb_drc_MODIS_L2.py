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

from PB import pb_name, pb_space
from PB.pb_sat import sun_earth_dis_correction
import numpy as np


class CLASS_MODIS_L2():
    """
    水色
    """

    def __init__(self):

        self.Ref = {}
        self.Lons = []
        self.Lats = []
        self.Time = []

        self.dset_list = [
            'Kd_490', 'chl_ocx', 'poc', 'Rrs_412', 'Rrs_443', 'Rrs_555', 'Rrs_488']
        self.dset_list2 = ['a_488_giop']
        self.dset_list3 = self.dset_list + self.dset_list2
        self.new_dset_list = [
            'Kd490', 'CHL1', 'POC', 'Rw412',
            'Rw443', 'Rw565', 'Rw490', 'a490']

    def Load(self, L1File):
        print (u'读取 L2 %s' % L1File)
        try:
            h5r = h5py.File(L1File, 'r')
            self.Lons = h5r.get('/navigation_data/longitude')[:]
            self.Lats = h5r.get('/navigation_data/latitude')[:]

            for key in self.dset_list:
                dataset = h5r.get('/geophysical_data/%s' % key)
                dataset_value = dataset[:]
                if 'Rrs' in key:
                    print key
                    condition = np.logical_or(
                        dataset_value <= -30000, dataset_value > 25000)
                elif 'Kd_490' in key:
                    condition = np.logical_or(
                        dataset_value < 50, dataset_value > 30000)
                elif 'chl_ocx' in key:
                    condition = np.logical_or(
                        dataset_value < 0.001, dataset_value > 100)
                elif 'poc' in key:
                    condition = np.logical_or(
                        dataset_value < -32000, dataset_value > -27000)
                value = dataset_value.astype(np.float32)
                # 过滤无效值
#                 condition = np.logical_or(value <= 0., value > 1000.)
                value[condition] = np.nan
                if 'chl_ocx' is not key:
                    slope = dataset.attrs['scale_factor']
                    intercept = dataset.attrs['add_offset']
                else:
                    slope = 1.
                    intercept = 0.
                new_value = value * slope + intercept
#                 new_value = (value - intercept) / slope
                self.Ref[key] = new_value

        except Exception as e:
            print str(e)

        # 处理定位文件，默认和L1同一目录  iop
        file_path = os.path.dirname(L1File)
        file_name = os.path.basename(L1File)
        p1, p2, p3 = file_name.split('_')
        file_name_iop = os.path.join(file_path, p1 + '_' + p2 + '_IOP.nc')
        print (u'读取 L2 %s' % file_name_iop)
        try:
            h5r = h5py.File(file_name_iop, 'r')

            for key in self.dset_list2:
                print key
                dataset = h5r.get('/geophysical_data/%s' % key)
                dataset_value = dataset[:]
                if 'a_488_giop' in key:
                    condition = np.logical_or(
                        dataset_value < -24999, dataset_value > -6072)
                value = dataset_value.astype(np.float32)
                # 过滤无效值
#                 condition = np.logical_or(value <= 0., value > 1000.)
                value[condition] = np.nan
                if 'chl_ocx' is not key:
                    slope = dataset.attrs['scale_factor']
                    intercept = dataset.attrs['add_offset']
                else:
                    slope = 1.
                    intercept = 0.
                print slope, intercept
                new_value = value * slope + intercept
                self.Ref[key] = new_value

        except Exception as e:
            print str(e)

        for key1, key2 in zip(self.new_dset_list, self.dset_list3):
            self.Ref[key1] = self.Ref.pop(key2)
if __name__ == '__main__':
    L1File = 'D:/data/modis_occ/A2013004003500.L2_LAC_OC.nc'
    modis = CLASS_MODIS_L2()
    modis.Load(L1File)
    for key in modis.Ref.keys():
        print key, np.nanmin(modis.Ref[key]), np.nanmax(modis.Ref[key])

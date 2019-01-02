# coding: utf-8

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


class CLASS_MERSI_L3():
    """
    水色
    """

    def __init__(self):

        self.Ref = {}
        self.satAzimuth = []
        self.satZenith = []
        self.sunAzimuth = []
        self.sunZenith = []
        self.Lons = []
        self.Lats = []
        self.Time = []

        self.dset_list = [
            'Kd490', 'Ocean_CHL1', 'POC', 'Zsd']

        self.new_dset_list = [
            'Kd490', 'CHL1', 'POC', 'ZSD']

    def Load(self, L1File):
        print (u'读取 L3 %s' % L1File)
        try:
            h5r = h5py.File(L1File, 'r')
            self.Lons = h5r.get('/Longitude')[:]
            self.Lats = h5r.get('/Latitude')[:]

            ary_satz = h5r.get('/SensorZenith')[:]
            ary_sata = h5r.get('/SensorAzimuth')[:]
            ary_sunz = h5r.get('/SolarZenith')[:]
            ary_suna = h5r.get('/SolarAzimuth')[:]

            self.satAzimuth = ary_sata / 100.
            self.satZenith = ary_satz / 100.
            self.sunAzimuth = ary_suna / 100.
            self.sunZenith = ary_sunz / 100.

            for key in self.dset_list:
                dataset = h5r.get('/%s' % key)
                value = dataset[:].astype(np.float32)
                # 过滤无效值
                condition = np.logical_or(value <= 0., value > 32767.)
                value[condition] = np.nan
                slope = dataset.attrs['Slope']
                intercept = dataset.attrs['Intercept']
                new_value = value * slope + intercept
                self.Ref[key] = new_value

        except Exception as e:
            print str(e)

        for key1, key2 in zip(self.new_dset_list, self.dset_list):
            self.Ref[key1] = self.Ref.pop(key2)


if __name__ == '__main__':
    L1File = 'D:/data/MERSI_OCC/FY3B_MERSI_ORBT_L2_OCC_MLT_NUL_20130101_0000_1000M.HDF'
    L1File = 'D:/data/MERSI_OCC/FY3B_MERSI_GBAL_L3_OCC_MLT_GLL_20130107_AOAD_5000M.HDF'
    modis = CLASS_MERSI_L3()
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
        ofile = os.path.join('D:/data/MERSI_OCC/', 'MERSI_%s.png' % Band)
        p.savefig(ofile)

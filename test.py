# coding=utf-8

import os
import re
import sys
import h5py
import numpy as np


__description__ = u'交叉主调度处理的函数'
__author__ = 'wangpeng'
__date__ = '2018-05-30'
__version__ = '1.0.0_beat'
__updated__ = '2018-07-09'


def main():
    ofile = 'test.h5'

    dn = np.full((2000, 2048), 3000, dtype='u2')
    Ref = np.full((2000, 2048), 3000, dtype='u2')
    Height = np.full((2000, 2048), 1, dtype='i2')
    Time = np.full((2000, 2048), 2, dtype='i4')
    LandCover = np.full((2000, 2048), 3, dtype='u1')
    LandSeaMask = np.full((2000, 2048), 4, dtype='u1')
    SensorAzimuth = np.full((2000, 2048), 5, dtype='i2')
    SensorZenith = np.full((2000, 2048), 6, dtype='i2')
    SolarAzimuth = np.full((2000, 2048), 7, dtype='i2')
    SolarZenith = np.full((2000, 2048), 8, dtype='i2')
    Latitude = np.full((2000, 2048), 9., dtype='f4')
    Longitude = np.full((2000, 2048), 10., dtype='f4')

    h5file_w = h5py.File(ofile, 'w')
    for i in xrange(20):
        bname = 'CH_%02d' % (i + 1)
        h5file_w.create_dataset(
            '%s/Ref' % bname, data=Ref, compression='gzip', compression_opts=5, shuffle=True)
        Ref = Ref + 1
    h5file_w.create_dataset(
        'Height', data=Height, compression='gzip', compression_opts=5, shuffle=True)
    h5file_w.create_dataset(
        'Time', data=Time, compression='gzip', compression_opts=5, shuffle=True)
    h5file_w.create_dataset(
        'LandCover', data=LandCover, compression='gzip', compression_opts=5, shuffle=True)
    h5file_w.create_dataset(
        'LandSeaMask', data=LandSeaMask, compression='gzip', compression_opts=5, shuffle=True)
    h5file_w.create_dataset('SensorAzimuth', data=SensorAzimuth,
                            compression='gzip', compression_opts=5, shuffle=True)
    h5file_w.create_dataset(
        'SensorZenith', data=SensorZenith, compression='gzip', compression_opts=5, shuffle=True)
    h5file_w.create_dataset(
        'SolarAzimuth', data=SolarAzimuth, compression='gzip', compression_opts=5, shuffle=True)
    h5file_w.create_dataset(
        'SolarZenith', data=SolarZenith, compression='gzip', compression_opts=5, shuffle=True)
    h5file_w.create_dataset(
        'Latitude', data=Latitude, compression='gzip', compression_opts=5, shuffle=True)
    h5file_w.create_dataset(
        'Longitude', data=Longitude, compression='gzip', compression_opts=5, shuffle=True)
    h5file_w.close()


if __name__ == '__main__':

    main()

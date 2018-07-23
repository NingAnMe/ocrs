#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/7/23 11:33
@Author  : AnNing
"""
import os

import h5py
import numpy as np

from PB import pb_io
from PB.pb_time import time_block
from DV import dv_map_oc
from DV.dv_img import dv_rgb
from DV.dv_pub_3d import plt


DEBUG = True
TIME_TEST = False


class ReadHDF5(object):
    """
    read_hdf5()
    read_groups()
    read_datasets()
    read_group()
    read_datasets()
    """
    def __init__(self, in_file):
        self.file_path = in_file
        self.dir_path = os.path.dirname(in_file)
        self.file_name = os.path.basename(in_file)

        self.ymd = None
        self.hm = None

        self.data = dict()
        self.file_attr = dict()
        self.data_attr = dict()

    def read_hdf5(self, datasets=None, groups=None):
        """
        :param datasets: [str] 需要读取的数据集
        :param groups: [str] 需要读取的数据组
        :return:
        """
        if datasets:
            self.read_datasets(datasets)
        if groups:
            self.read_groups(groups)
        if datasets is None and groups is None:
            self.read_all()

    def read_all(self):
        with h5py.File(self.file_path, 'r') as hdf5_file:
            for item in hdf5_file:
                if type(hdf5_file[item]).__name__ == 'Group':
                    hdf5_group = hdf5_file[item]
                    self.read_group(hdf5_group)
                else:
                    hdf5_dataset = hdf5_file[item]
                    self.read_dataset(hdf5_dataset)

    def read_groups(self, groups):
        """
        :param groups: [str] 需要读取的数据组
        :return:
        """
        with h5py.File(self.file_path, 'r') as hdf5_file:
            for group in groups:
                hdf5_group = hdf5_file[group]
                self.read_group(hdf5_group)

    def read_datasets(self, datasets):
        """
        :param datasets: [str] 需要读取的数据集
        :return:
        """
        with h5py.File(self.file_path, 'r') as hdf5_file:
            for dataset in datasets:
                hdf5_dataset = hdf5_file[dataset]
                self.read_dataset(hdf5_dataset)

    def read_group(self, hdf5_group):
        for item in hdf5_group:
            if type(hdf5_group[item]).__name__ == 'Group':
                hdf5_group = hdf5_group[item]
                self.read_group(hdf5_group)
            else:
                hdf5_dataset = hdf5_group[item]
                self.read_dataset(hdf5_dataset)

    def read_dataset(self, hdf5_dataset):
        dataset_path = hdf5_dataset.name.split('/')
        dataset_name = dataset_path.pop()
        data = self._create_data_dict(dataset_path)
        data[dataset_name] = hdf5_dataset.value

    def _create_data_dict(self, dataset_path):
        """
        :param dataset_path: [str]
        :return: dict
        """
        data = self.data
        for i in dataset_path:
            if not i:
                continue
            if i in data:
                data = data[i]
                continue
            else:
                data[i] = {}
                data = data[i]
        return data

    # TODO
    read

if __name__ == '__main__':
    test_file = r'E:\projects\oc_data\FY3B+MERSI_AQUA+MODIS_MATCHEDPOINTS_20130119213330.H5'
    read_hdf5 = ReadHDF5(test_file)
    read_hdf5.read_hdf5()
    keys = read_hdf5.data.keys()
    keys.sort()

    read_hdf5 = ReadHDF5(test_file)
    read_hdf5.read_hdf5(datasets=[u'/CH_01/S1_FovRefMean'], groups=[u'CH_01'])
    keys = read_hdf5.data.keys()
    keys.sort()
    print keys
    print read_hdf5.data

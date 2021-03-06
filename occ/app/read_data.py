#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/7/23 11:33
@Author  : AnNing
"""
import os

import h5py
import numpy as np

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

    # TODO 添加读取属性的方法
    def read_file_attr(self):
        pass

    def read_dataset_attr(self):
        pass


class ReadCrossDataL2(object):
    """
    读取L2的交叉匹配信息
    """
    def __init__(self):
        self.data = dict()

    def read_cross_data(self, in_files):

        for in_file in in_files:
            read_hdf5 = ReadHDF5(in_file)
            read_hdf5.read_hdf5()
            print in_file
            self.concatenate(read_hdf5.data, axis=0)

    def concatenate(self, data, axis=0):
        for channel in data:
            if isinstance(data[channel], dict):
                mask_fine = data[channel]["MaskFine"]
                idx_fine = np.where(mask_fine > 0)
                if len(idx_fine[0]) <= 0:
                    continue
                ref_s1 = data[channel]["MERSI_FovMean"][idx_fine]
                ref_s2 = data[channel]["MODIS_FovMean"][idx_fine]
                lon_s1 = data["MERSI_Lats"][idx_fine]
                lat_s1 = data["MERSI_Lons"][idx_fine]

                if channel not in self.data:
                    self.data[channel] = dict()
                    self.data[channel]["MERSI_FovMean"] = ref_s1
                    self.data[channel]["MODIS_FovMean"] = ref_s2
                    self.data[channel]["MERSI_Lons"] = lon_s1
                    self.data[channel]["MERSI_Lats"] = lat_s1
                else:
                    d_c = self.data[channel]
                    d_c["MERSI_FovMean"] = np.concatenate((d_c["MERSI_FovMean"], ref_s1), axis=axis)
                    d_c["MODIS_FovMean"] = np.concatenate((d_c["MODIS_FovMean"], ref_s2), axis=axis)
                    d_c["MERSI_Lons"] = np.concatenate((d_c["MERSI_Lons"], lon_s1), axis=axis)
                    d_c["MERSI_Lats"] = np.concatenate((d_c["MERSI_Lats"], lat_s1), axis=axis)


class ReadCrossData(object):
    """
    读取交叉匹配数据
    """

    def __init__(self):
        self.ymd = None
        self.hm = None

        self.data = dict()
        self.file_attr = dict()
        self.data_attr = dict()

    def read_cross_data(self, in_files):

        for in_file in in_files:
            read_hdf5 = ReadHDF5(in_file)
            read_hdf5.read_hdf5()
            if not self.data:
                self.data = read_hdf5.data
            else:
                self.concatenate(read_hdf5.data, axis=0)

    def concatenate(self, data, axis=None):
        """
        仅支持2层数据
        :param axis:
        :param data:
        :return:
        """
        for key in self.data:
            if isinstance(self.data[key], dict):
                for key_two in self.data[key]:
                    self.data[key][key_two] = np.concatenate(
                        (self.data[key][key_two], data[key][key_two]), axis=axis)
            else:
                self.data[key] = np.concatenate(
                    (self.data[key], data[key]), axis=axis)


if __name__ == '__main__':
    test_file = r'E:\projects\oc_data\FY3B+MERSI_AQUA+MODIS_MATCHEDPOINTS_20130119213330.H5'
    # read_hdf5 = ReadHDF5(test_file)
    # read_hdf5.read_hdf5()
    # keys = read_hdf5.data.keys()
    # keys.sort()
    #
    # read_hdf5 = ReadHDF5(test_file)
    # read_hdf5.read_hdf5(datasets=[u'/CH_01/S1_FovRefMean'], groups=[u'CH_01'])
    # keys = read_hdf5.data.keys()
    # keys.sort()
    # print keys
    # print read_hdf5.data

    read_cross_data = ReadCrossData()
    read_cross_data.read_cross_data(in_files=[test_file, test_file])
    print read_cross_data.data

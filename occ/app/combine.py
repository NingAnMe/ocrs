#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/6/8 14:55
@Author  : AnNing
"""
import gc
import os
from datetime import datetime

import h5py
import yaml

from DP.dp_prj import prj_core
from PB import pb_io
from PB.pb_time import time_block
import numpy as np


TIME_TEST = False  # 运行时间测试


class CombineL2(object):
    """
    对水色产品 L2 产品进行合成
    """

    def __init__(self):
        self.error = False

        self.res = None
        self.cmd = None
        self.col = None
        self.row = None

        self.yaml_file = None

        self.ifile = None
        self.pfile = None
        self.ofile = None

        self.in_data = {}
        self.attrs = {}
        self.out_data = {}

        self.lons = None
        self.lats = None

        self.lookup_table = None

        self.ii = list()
        self.jj = list()
        self.data_ii = list()
        self.data_jj = list()
        self.ijd = list()

        self.lut_ii = list()
        self.lut_jj = list()
        self.data_ij = dict()

    def load_cmd_info(self, cmd=None, res=None, row=None, col=None):
        """
        获取拼接投影命令的参数信息
        :param cmd:
        :param res:
        :param row:
        :param col:
        :return:
        """
        if self.error:
            return
        self.cmd = cmd
        self.res = res
        self.row = row
        self.col = col

    def load_yaml(self, yaml_file):
        """
        读取 yaml 格式配置文件
        """
        if self.error:
            return
        if not os.path.isfile(yaml_file):
            print 'Not Found %s' % yaml_file
            self.error = True
            return

        self.yaml_file = yaml_file
        try:
            with open(yaml_file, 'r') as stream:
                cfg = yaml.load(stream)

            self.ifile = sorted(cfg['PATH']['ipath'])
            self.pfile = sorted(cfg['PATH']['ppath'])
            self.ofile = cfg['PATH']['opath']

        except Exception as why:
            print why
            print "Load yaml file error, please check it. : {}".format(yaml_file)
            self.error = True

    def load_proj_data(self, hdf5_file):
        if self.error:
            return
        # 加载数据
        if os.path.isfile(hdf5_file):
            try:
                with h5py.File(hdf5_file, 'r') as h5:
                    lut_ii = h5.get("lut_ii")[:]
                    lut_jj = h5.get("lut_jj")[:]
                    data_ii = h5.get("data_ii")[:]
                    data_jj = h5.get("data_jj")[:]

            except Exception as why:
                print why
                print "Can't open file: {}".format(hdf5_file)
                self.error = True
                return
        else:
            print "File does not exist: {}".format(hdf5_file)
            self.error = True
            return
        return lut_ii, lut_jj, data_ii, data_jj

    def load_proj_files(self, in_files):
        for in_file in in_files:
            print '<<< {}'.format(in_file)
            datas = self.load_proj_data(in_file)
            if datas is not None:
                lut_ii, lut_jj, data_ii, data_jj = datas
                filename = os.path.basename(in_file)
                k = filename.split('_5000M')[0]
                self.lut_ii = np.append(self.lut_ii, lut_ii)
                self.lut_jj = np.append(self.lut_jj, lut_jj)
                self.data_ij[k] = (data_ii, data_jj)

    def combine(self):
        all_time_start = datetime.utcnow()
        if self.error:
            return
        # 如果输出文件已经存在，跳过
        elif os.path.isfile(self.ofile):
            self.error = True
            print "File is already exist, skip it: {}".format(self.ofile)
            return
        # 合成日数据
        elif pb_io.is_none(self.ifile, self.pfile, self.ofile):
            self.error = True
            print "Is None: ifile or pfile or ofile: {}".format(self.yaml_file)
            return
        elif len(self.ifile) < 1:
            self.error = True
            print "File count lower than 1: {}".format(self.yaml_file)

        self.load_proj_files(self.pfile)  # 读取投影文件

        fill_value = -32767

        with h5py.File(self.ifile[0], 'r') as h5:
            data_names = h5.keys()
            data_names.sort()

            for k in h5.keys():
                # 记录属性信息
                if k not in self.attrs:
                    self.attrs[k] = pb_io.attrs2dict(h5.get(k).attrs)

        file_count = len(self.ifile)
        data_count = list()
        date_start = datetime.utcnow()
        for data_name in data_names:
            if data_name == "Longitude" or data_name == "Latitude" or data_name == "Ocean_Flag":
                continue
            if 'Azimuth' in data_name or 'Zenith' in data_name or data_name == 'chl':
                continue

            data = list()
            count = 0
            # 读取所有的数据
            for in_file in self.ifile:
                filename = os.path.basename(in_file)
                k = filename.split('_1000M')[0]
                data_ii, data_jj = self.data_ij[k]
                with h5py.File(in_file, 'r') as hdf5:
                    data_read = hdf5.get(data_name)[:][data_ii, data_jj]
                    data = np.append(data, data_read)
                date_end = datetime.utcnow() - date_start
                count += 1
                print 'Combine {}  {}/{} {}'.format(data_name, count, file_count, date_end)

            condition = np.logical_and(data != 32767, data != -32767)
            condition = np.logical_and(condition, data != 0)
            lut_ii = self.lut_ii[condition]
            lut_jj = self.lut_jj[condition]
            data = data[condition]

            data_count.append((data_name, len(data)))

            if not len(data) == 0:
                ijd = dict()
                date_start = datetime.utcnow()
                for i, j, d in zip(lut_ii, lut_jj, data):
                    local = (i, j)
                    if local not in ijd:
                        ijd[local] = [d]
                    else:
                        data_list = ijd[local]
                        data_list.append(d)
                date_end = datetime.utcnow() - date_start
                print date_end

                local = ijd.keys()
                local = np.array(local)
                lut_ii = local[:, 0].astype(np.int16)
                lut_jj = local[:, 1].astype(np.int16)

                data = ijd.values()

                date_start = datetime.utcnow()
                mean_ = np.vectorize(mean_data)
                data = mean_(data)
                date_end = datetime.utcnow() - date_start
                print date_end

                out_data = np.full((self.row, self.col), fill_value, dtype='i2')
                data = data.reshape(-1)
                out_data[lut_ii, lut_jj] = data
            else:
                out_data = np.full((self.row, self.col), fill_value, dtype='i2')
            self.write(out_data, data_name, self.ofile)

        # 输出经纬度数据集
        lookup_table = prj_core(self.cmd, self.res, unit="deg", row=self.row, col=self.col)
        lookup_table.grid_lonslats()
        longitude = lookup_table.lons
        latitude = lookup_table.lats
        data_name = "Longitude"
        self.write(longitude, data_name, self.ofile)
        data_name = "Latitude"
        self.write(latitude, data_name, self.ofile)

        # 输出海陆标记和方位角
        for data_name in ['SensorAzimuth', 'SensorZenith', 'SolarAzimuth',
                          'SolarZenith', 'Ocean_Flag', 'chl']:
            out_data = np.full((self.row, self.col), fill_value, dtype='i4')
            data = list()
            for in_file in self.ifile:
                filename = os.path.basename(in_file)
                k = filename.split('_1000M')[0]
                data_ii, data_jj = self.data_ij[k]
                with h5py.File(in_file, 'r') as hdf5:
                    data_read = hdf5.get(data_name)[:][data_ii, data_jj]
                    data = np.append(data, data_read)
            data = np.reshape(data, -1)
            lut_ii = self.lut_ii.astype(np.int16)
            lut_jj = self.lut_jj.astype(np.int16)
            out_data[lut_ii, lut_jj] = data
            self.write(out_data, data_name, self.ofile)

        # 输出数据集有效数据的数量
        for count in data_count:
            print "{:30} : {}".format(count[0], count[1])

        all_time_end = datetime.utcnow() - all_time_start
        print all_time_end

    def write(self, data, data_name, out_file):
        if self.error:
            return
        pb_io.make_sure_path_exists(os.path.dirname(self.ofile))
        # 写入 HDF5 文件
        with h5py.File(out_file, 'a') as h5:

            if data_name == "Longitude" or data_name == "Latitude":
                h5.create_dataset(data_name, dtype='f4',
                                  data=data,
                                  compression='gzip', compression_opts=5,
                                  shuffle=True)
            elif data_name == "Ocean_Flag":
                h5.create_dataset(data_name, dtype='i4',
                                  data=data,
                                  compression='gzip', compression_opts=5,
                                  shuffle=True)
            else:
                h5.create_dataset(data_name, dtype='i2',
                                  data=data,
                                  compression='gzip', compression_opts=5,
                                  shuffle=True)

            # 复制属性
            attrs = self.attrs[data_name]
            for key, value in attrs.items():
                h5[data_name].attrs[key] = value


class CombineL2Quick(object):
    """
    对水色产品 L2 产品进行合成
    """

    def __init__(self):
        self.error = False

        self.res = None
        self.cmd = None
        self.col = None
        self.row = None

        self.yaml_file = None

        self.ifile = None
        self.pfile = None
        self.ofile = None

        self.in_data = {}
        self.attrs = {}
        self.out_data = {}

        self.lons = None
        self.lats = None

        self.lookup_table = None

        self.ii = None
        self.jj = None

        self.lut_ii = None
        self.lut_jj = None
        self.data_ii = None
        self.data_jj = None

    def load_cmd_info(self, cmd=None, res=None, row=None, col=None):
        """
        获取拼接投影命令的参数信息
        :param cmd:
        :param res:
        :param row:
        :param col:
        :return:
        """
        if self.error:
            return
        self.cmd = cmd
        self.res = res
        self.row = row
        self.col = col

    def load_yaml(self, yaml_file):
        """
        读取 yaml 格式配置文件
        """
        if self.error:
            return
        if not os.path.isfile(yaml_file):
            print 'Not Found %s' % yaml_file
            self.error = True
            return

        self.yaml_file = yaml_file
        try:
            with open(yaml_file, 'r') as stream:
                cfg = yaml.load(stream)

            self.ifile = cfg['PATH']['ipath']
            self.pfile = cfg['PATH']['ppath']
            self.ofile = cfg['PATH']['opath']

        except Exception as why:
            print why
            print "Load yaml file error, please check it. : {}".format(yaml_file)
            self.error = True

    def load_proj_data(self, hdf5_file):
        if self.error:
            return
        # 加载数据
        if os.path.isfile(hdf5_file):
            try:
                with h5py.File(hdf5_file, 'r') as h5:
                    self.lut_ii = h5.get("lut_ii")[:]
                    self.lut_jj = h5.get("lut_jj")[:]
                    self.data_ii = h5.get("data_ii")[:]
                    self.data_jj = h5.get("data_jj")[:]

            except Exception as why:
                print why
                print "Can't open file: {}".format(hdf5_file)
                self.error = True
                return
        else:
            print "File does not exist: {}".format(hdf5_file)
            self.error = True
            return
        return

    def combine(self):
        if self.error:
            return

        # 如果输出文件已经存在，跳过
        elif os.path.isfile(self.ofile):
            self.error = True
            print "File is already exist, skip it: {}".format(self.ofile)
            return
        # 合成日数据
        elif pb_io.is_none(self.ifile, self.pfile, self.ofile):
            self.error = True
            print "Is None: ifile or pfile or ofile: {}".format(self.yaml_file)
            return
        elif len(self.ifile) < 1:
            self.error = True
            print "File count lower than 1: {}".format(self.yaml_file)

        fill_value = -32767
        for file_idx, in_file in enumerate(self.ifile):
            proj_file = self.pfile[file_idx]
            if os.path.isfile(in_file) and os.path.isfile(proj_file):
                print "*" * 100
                print "Start combining file:"
                print "<<< {}\n<<< {}".format(in_file, proj_file)
            else:
                print "File is not exist: {} OR {}".format(in_file, proj_file)
                continue

            # 加载 proj 数据
            self.load_proj_data(proj_file)
            # 日合成
            with time_block("One combine time:", switch=TIME_TEST):
                try:
                    with h5py.File(in_file, 'r') as h5:
                        for k in h5.keys():
                            # 记录属性信息
                            if k not in self.attrs:
                                self.attrs[k] = pb_io.attrs2dict(
                                    h5.get(k).attrs)

                            if k == "Longitude" or k == "Latitude":
                                continue
                            elif k not in self.out_data:
                                if k == "Ocean_Flag":
                                    self.out_data[k] = np.full((self.row, self.col), fill_value,
                                                               dtype='i4')
                                else:
                                    self.out_data[k] = np.full((self.row, self.col), fill_value,
                                                               dtype='i2')
                            # 合并一个数据
                            proj_data = h5.get(k)[:]
                            self.out_data[k][self.lut_ii, self.lut_jj] = proj_data[
                                self.data_ii, self.data_jj]

                except Exception as why:
                    print why
                    print "Can't combine file, some error exist: {}".format(in_file)

        with time_block("Grid to lons and lats time:", switch=TIME_TEST):
            if "Longitude" not in self.out_data:
                lookup_table = prj_core(
                    self.cmd, self.res, unit="deg", row=self.row, col=self.col)
                lookup_table.grid_lonslats()
                self.out_data["Longitude"] = lookup_table.lons
                self.out_data["Latitude"] = lookup_table.lats

        # 输出数据集有效数据的数量
        keys = [x for x in self.out_data]
        keys.sort()
        for k in keys:
            if self.out_data[k] is None:
                print k
                continue
            idx = np.where(self.out_data[k] > 0)
            print "{:30} : {}".format(k, len(idx[0]))

    def write(self):
        if self.error:
            return
        pb_io.make_sure_path_exists(os.path.dirname(self.ofile))
        # 写入 HDF5 文件
        with h5py.File(self.ofile, 'w') as h5:
            for k in self.out_data:
                # 创建数据集
                if k == "Longitude" or k == "Latitude":
                    h5.create_dataset(k, dtype='f4',
                                      data=self.out_data[k],
                                      compression='gzip', compression_opts=5,
                                      shuffle=True)
                elif k == "Ocean_Flag":
                    h5.create_dataset(k, dtype='i4',
                                      data=self.out_data[k],
                                      compression='gzip', compression_opts=5,
                                      shuffle=True)
                else:
                    h5.create_dataset(k, dtype='i2',
                                      data=self.out_data[k],
                                      compression='gzip', compression_opts=5,
                                      shuffle=True)

                # 复制属性
                attrs = self.attrs[k]
                for key, value in attrs.items():
                    h5[k].attrs[key] = value


class CombineL3(object):
    """
    对水色产品的L3数据进行合成
    """

    def __init__(self):
        self.error = False

        self.yaml_file = None

        self.ifile = None
        self.ofile = None

        self.in_data = {}
        self.attrs = {}
        self.out_data = {}
        self.counter = {}
        self.fill_value = -32767

        self.one_in_file = None

    def load_yaml(self, yaml_file):
        """
        读取 yaml 格式配置文件
        """
        if self.error:
            return
        if not os.path.isfile(yaml_file):
            print 'Not Found %s' % yaml_file
            self.error = True
            return

        self.yaml_file = yaml_file
        try:
            with open(yaml_file, 'r') as stream:
                cfg = yaml.load(stream)

            self.ifile = cfg['PATH']['ipath']
            self.ofile = cfg['PATH']['opath']

        except Exception as why:
            print why
            print "Error: Load yaml file error, please check it. : {}".format(yaml_file)
            self.error = True

    def _3d_data_calculate(self):
        """
        计算数据的平均值
        :return:
        """
        if self.error:
            return
        for k in self.out_data:
            if k == "Longitude" or k == "Latitude" or k == "Ocean_Flag":
                continue
            data = self.out_data[k]
            del self.out_data[k]
            gc.collect()
            data = np.ma.masked_less_equal(data, 0)
            data = np.mean(data, axis=2).astype(np.int16)
            self.out_data[k] = np.ma.filled(data, self.fill_value)
            del data
            gc.collect()

    def _combine_3d(self):
        """
        将二维数据合成三维数据
        """
        if self.error:
            return
        try:
            with h5py.File(self.one_in_file, 'r') as h5:
                for k in h5.keys():
                    shape = h5.get(k).shape
                    reshape = (shape[0], shape[1], 1)
                    if k not in self.out_data:  # 创建输出数据集和计数器
                        if k == "Ocean_Flag":
                            continue
                        elif k == "Longitude" or k == "Latitude":
                            self.out_data[k] = h5.get(k)[:]
                        else:
                            data = h5.get(k)[:].reshape(reshape)
                            self.out_data[k] = data

                    else:
                        if k == "Longitude" or k == "Latitude" or k == "Ocean_Flag":
                            continue
                        else:
                            data = h5.get(k)[:].reshape(reshape)
                            self.out_data[k] = np.concatenate(
                                (self.out_data[k], data), axis=2)

                    # 记录属性信息
                    if k in self.attrs:
                        continue
                    else:
                        self.attrs[k] = pb_io.attrs2dict(h5.get(k).attrs)

        except Exception as why:
            print why
            print "Can't combine file, some error exist: {}".format(self.one_in_file)

    def _2d_data_calculate(self):
        """
        计算数据的平均值
        :return:
        """
        if self.error:
            return
        for k, counter in self.counter.items():
            idx = np.where(counter > 0)
            self.out_data[k][idx] = self.out_data[k][idx] / counter[idx]
            idx_fill = np.less_equal(self.out_data[k], 0)
            self.out_data[k][idx_fill] = self.fill_value

    def _combine_2d(self):
        """
        将日数据合成为2维数据，然后计算均值
        """
        if self.error:
            return
        try:
            with h5py.File(self.one_in_file, 'r') as h5:
                for k in h5.keys():
                    if k not in self.out_data:  # 创建输出数据集和计数器
                        shape = h5.get(k).shape
                        if k == "Ocean_Flag":
                            continue
                        elif k == "Longitude" or k == "Latitude":
                            self.out_data[k] = h5.get(k)[:]
                        else:
                            self.out_data[k] = np.zeros(shape, dtype='i2')
                            self.counter[k] = np.zeros(shape, dtype='i2')
                    if k == "Longitude" or k == "Latitude" or k == "Ocean_Flag":
                        continue
                    else:
                        value = h5.get(k)[:]
                        idx = np.where(value > 0)
                        self.out_data[k][idx] = self.out_data[
                            k][idx] + value[idx]
                        self.counter[k][idx] += 1

                    # 记录属性信息
                    if k not in self.attrs:
                        self.attrs[k] = pb_io.attrs2dict(h5.get(k).attrs)

        except Exception as why:
            print why
            print "Can't combine file, some error exist: {}".format(self.one_in_file)

    def _print_data_count(self):
        """
        打印有效数据的数量
        """
        keys = [x for x in self.out_data]
        keys.sort()
        for k in keys:
            if self.out_data[k] is None:
                print k
                continue
            idx = np.where(self.out_data[k] > 0)
            print "{:30} : {}".format(k, len(idx[0]))

    def combine(self):
        if self.error:
            return

        # 如果输出文件已经存在，跳过
        elif os.path.isfile(self.ofile):
            self.error = True
            print "Error: File is already exist, skip it: {}".format(self.ofile)
            return
        # 合成日数据
        elif pb_io.is_none(self.ifile, self.ofile):
            self.error = True
            print "Error: Is None: ifile or ofile: {}".format(self.yaml_file)
            return
        elif len(self.ifile) < 1:
            self.error = True
            print "Error: File count lower than 1: {}".format(self.yaml_file)
            return

        for in_file in self.ifile:
            if os.path.isfile(in_file):
                print "<<< {}".format(in_file)
            else:
                print "Warning: File is not exist: {}".format(in_file)
                continue
            self.one_in_file = in_file
            # 日合成
            with time_block("One combine time:", switch=TIME_TEST):
                self._combine_2d()

        # 计算数据的平均值
        with time_block("Calculate mean time:", switch=TIME_TEST):
            print "Start calculate."
            self._2d_data_calculate()

        # 输出数据集有效数据的数量
        self._print_data_count()

    def write(self):
        if self.error:
            return
        pb_io.make_sure_path_exists(os.path.dirname(self.ofile))
        # 写入 HDF5 文件
        with h5py.File(self.ofile, 'w') as h5:
            for k in self.out_data:
                # 创建数据集
                if k == "Longitude" or k == "Latitude":
                    h5.create_dataset(k, dtype='f4',
                                      data=self.out_data[k],
                                      compression='gzip', compression_opts=5,
                                      shuffle=True)
                elif k == "Ocean_Flag":
                    continue
                else:
                    h5.create_dataset(k, dtype='i2',
                                      data=self.out_data[k],
                                      compression='gzip', compression_opts=5,
                                      shuffle=True)

                # 复制属性
                if k == "Longitude" or k == "Latitude" or k == "Ocean_Flag":
                    continue
                attrs = self.attrs[k]
                for key, value in attrs.items():
                    h5[k].attrs[key] = value


def mean_data(data):
    return np.mean(data)

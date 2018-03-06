# coding:utf-8
"""
pm_h5py.py
hdf5 处理相关函数
~~~~~~~~~~~~~~~~~~~
creation time : 2018 1 19
author : anning
~~~~~~~~~~~~~~~~~~~
"""

import os
import sys
import logging
import re
from datetime import datetime
from posixpath import join

import h5py
import numpy as np
from configobj import ConfigObj


def read_dataset_hdf5(file_path, set_name):
    """
    读取 hdf5 文件，返回一个 numpy 多维数组
    :param file_path: (unicode)文件路径
    :param set_name: (str or list)表的名字
    :return: 如果传入的表名字是一个字符串，返回 numpy.ndarray
             如果传入的表名字是一个列表，返回一个字典，key 是表名字， value 是 numpy.ndarry
    """
    if isinstance(set_name, str):
        if os.path.isfile(file_path):
            file_h5py = h5py.File(file_path, 'r')
            data = file_h5py.get(set_name)[:]
            dataset = np.array(data)
            file_h5py.close()
            return dataset
        else:
            raise ValueError('value error: file_path')
    elif isinstance(set_name, list):
        datasets = {}
        if os.path.isfile(file_path):
            file_h5py = h5py.File(file_path, 'r')
            for name in set_name:
                data = file_h5py.get(name)[:]
                dataset = np.array(data)
                datasets[name] = dataset
            file_h5py.close()
            return datasets
        else:
            raise ValueError('value error: file_path')
    else:
        raise ValueError('value error: set_name')


def read_attr_hdf5(file_path, set_name, attr_name):
    """
    读取 hdf5 文件，返回属性值
    :param file_path: (unicode)文件路径
    :param set_name: (str)表的名字
    :param attr_name: (str or list)属性的名字
    :return: 如果传入的属性的名字是一个字符串，返回对应的属性值
             如果传入的属性的名字是一个列表，返回一个字典，key 是属性名， value 是对应的属性值
    """
    if isinstance(attr_name, str):
        if os.path.isfile(file_path):
            file_h5py = h5py.File(file_path, 'r')
            set_h5py = file_h5py.get(set_name)
            attr = set_h5py.attrs.get(attr_name)
            file_h5py.close()
            return attr
        else:
            raise ValueError('value error: file_path')
    elif isinstance(attr_name, list):
        attrs = {}
        if os.path.isfile(file_path):
            file_h5py = h5py.File(file_path, 'r')
            set_h5py = file_h5py.get(set_name)
            for name in attr_name:
                attr = set_h5py.attrs.get(name)
                attrs[name] = attr
            file_h5py.close()
            return attrs
        else:
            raise ValueError('value error: file_path')
    else:
        raise ValueError('value error: attr_name')


def modify_dataset_name_hdf5(file_path, old_name, new_name):
    """
    修改 hdf5 文件的表名称
    :param file_path: (unicode)文件路径
    :param old_name: 旧名称
    :param new_name: 新名称
    :return: 
    """
    if os.path.isfile(file_path):
        file_h5py = h5py.File(file_path, 'a')
        file_h5py.move(old_name, new_name)
        file_h5py.close()
    else:
        raise ValueError('value error：file_path')


def copy_attrs_h5py(pre_object, out_object):
    """
    复制 dataset 或者 group 的属性
    :param pre_object: 被复制属性的 dataset 或者 group
    :param out_object: 复制属性的 dataset 或者 group
    :return:
    """
    for akey in pre_object.attrs.keys():
        out_object.attrs[akey] = pre_object.attrs[akey]


def compress_hdf5(pre_hdf5, out_dir=None, out_file=None, level=5):
    """
    对 hdf5 文件进行压缩
    :param pre_hdf5: 输入文件
    :param out_dir: 输出路径，文件名与原来相同
    :param out_file: 输出文件，使用输出文件名
    :param level: 压缩等级
    :return:
    """
    if not os.path.isfile(pre_hdf5):
        raise ValueError('is not a file')
    if out_dir is not None:
        path, name = os.path.split(pre_hdf5)
        new_hdf5 = os.path.join(out_dir, name)
    elif out_file is not None:
        new_hdf5 = out_file
    else:
        raise ValueError('outpath and outfile value is error')
    pre_hdf5 = h5py.File(pre_hdf5, 'r')
    new_hdf5 = h5py.File(new_hdf5, 'w')

    compress(pre_hdf5, new_hdf5, level)

    pre_hdf5.close()
    new_hdf5.close()


def compress(pre_object, out_object, level=5):
    """
    对 h5df 文件进行深复制，同时对数据表进行压缩
    :param pre_object: 
    :param out_object:
    :param level:
    :return: 
    """
    for key in pre_object.keys():
        pre_dateset = pre_object.get(key)

        if type(pre_dateset).__name__ == "Group":
            out_dateset = out_object.create_group(key)
            compress(pre_dateset, out_dateset)
        else:
            out_dateset = out_object.create_dataset(key, dtype=pre_dateset.dtype, data=pre_dateset,
                                                    compression='gzip', compression_opts=level,  # 压缩等级5
                                                    shuffle=True)
            # 复制dataset属性
            for akey in pre_dateset.attrs.keys():
                out_dateset.attrs[akey] = pre_dateset.attrs[akey]

    # 复制group属性
    for akey in pre_object.attrs.keys():
        out_object.attrs[akey] = pre_object.attrs[akey]

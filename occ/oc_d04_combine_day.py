#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/12/11 10:24
@Author  : yushuai
"""

from datetime import datetime
import os
import pdb
import re
import sys

from dateutil.relativedelta import relativedelta
from numba.types.containers import Pair
import h5py
import yaml

from PB.CSC.pb_csc_console import LogServer
from PB.pb_io import Config, make_sure_path_exists
from PB.pb_time import ymd2date, time_block
from app.bias import Bias
from app.config import InitApp
from app.plot import plot_time_series
import numpy as np


TIME_TEST = False  # 时间测试
RED = '#f63240'
BLUE = '#1c56fb'
GRAY = '#c0c0c0'
EDGE_GRAY = '#303030'


def write_h5(ofile, data):

    out_path = os.path.dirname(ofile)
    if not os.path.isdir(out_path):
        os.makedirs(out_path)

    # 打开h5文件
    h5w = h5py.File(ofile, 'w')

    for key in sorted(data.keys()):
        h5w.create_dataset(key, data=data[key])

    h5w.close()


class LoadH5():
    """
    read hdf5类型数据到字典中，支持合并
    """

    def __init__(self):
        self.data = {}

    def load(self, in_file, ymd):
        try:
            h5File_R = h5py.File(in_file, 'r')
            if len(h5File_R.keys()) > 0:
                if 'ymd' not in self.data.keys():
                    self.data['ymd'] = []
#                 ary_ymd = (np.array(int(ymd))).reshape(1, 1)
                self.data['ymd'].append(ymd)

            for key in h5File_R.keys():
                root_id = h5File_R.get(key)
                if type(root_id).__name__ == "Group":  # 判断名字是否属于组
                    if key not in self.data.keys():
                        self.data[key] = {}
                    for dkey in root_id.keys():
                        h5data = root_id.get(dkey)[:]

                        if dkey not in self.data[key].keys():

                            self.data[key][dkey] = h5data
                        else:
                            self.data[key][dkey] = np.concatenate(
                                (self.data[key][dkey], h5data))
                else:
                    h5data = h5File_R.get(key)[:]
                    if key not in self.data.keys():
                        self.data[key] = h5data
                    else:
                        self.data[key] = np.concatenate(
                            (self.data[key], h5data))

            h5File_R.close()
        except Exception as e:
            print str(e)
            print "Load file error: {}".format(in_file)


def main(yamlfile):
    """
    对L3产品做长时间序列图
    """
    YamlFile = yamlfile
    if not os.path.isfile(YamlFile):
        print 'Not Found %s' % YamlFile
        sys.exit(-1)

    with open(YamlFile, 'r') as stream:
        cfg = yaml.load(stream)

    ymd = cfg["INFO"]['ymd']
    pair = cfg["INFO"]['pair']
    file_list = cfg["PATH"]['ipath']
    out_path = cfg["PATH"]['opath']

    print "-" * 100
#     date_start = ymd2date(s_time)
#     date_end = ymd2date(e_time)

    for in_file in file_list:
        h5 = LoadH5()

        data_dict = {}

        datetime_list = []
        file_name = os.path.basename(in_file)
        if 'L3' in pair:
            reg = '.*_(\d{8}).*.HDF'
            m = re.match(reg, file_name)
            ymd = m.group(1)
            datetime_list.append(ymd2date(ymd))
        elif 'L2' in pair:
            reg = '.*_(\d{14}).*.HDF'
            m = re.match(reg, file_name)
            ymd = m.group(1)
            ymdhms = datetime.strptime('%s' % (ymd), "%Y%m%d%H%M%S")
            datetime_list.append(ymdhms)

        h5.load(in_file, ymd)
        dict_data = {}
        for key in h5.data.keys():
            if key in ['Longitude', 'ymd', 'Latitude']:
                continue
            data = h5.data[key]
            idx = np.where(data > 0)
            mean = np.mean(data[idx])
#             print key, type(mean)
            dict_data[key] = mean.reshape(1, 1)
#         print dict_data

        file_name = '%s_%s_COMBINE.HDF' % (pair, ymd)
        ofile = os.path.join(out_path, file_name)
        write_h5(ofile, dict_data)

#


# ######################## 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    ARGS = sys.argv[1:]
    HELP_INFO = \
        u"""
        [arg1]：sat+sensor
        [arg2]：yaml file
        [arg3]: is_time_series [bool]
        [example]： python app.py arg1 arg2
        """
    if "-h" in ARGS:
        print HELP_INFO
        sys.exit(-1)

    if len(ARGS) == 2:
        SAT_SENSOR = ARGS[0]
        FILE_PATH = ARGS[1]

        with time_block("All", switch=TIME_TEST):
            main(FILE_PATH)
    else:
        print HELP_INFO
        sys.exit(-1)

# ######################### TEST ##############################
# if __name__ == '__main__':
#     yaml_file = r'D:\nsmc\occ_data\20130103154613_MERSI_MODIS.yaml'
#     main('FY3B+MERSI_AQUA+MODIS', yaml_file)

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

    s_time = cfg["INFO"]['stime']
    e_time = cfg["INFO"]['etime']
    pair = cfg["INFO"]['pair']
    file_list = cfg["PATH"]['ipath']
    out_path = cfg["PATH"]['opath']

    print "-" * 100
#     date_start = ymd2date(s_time)
#     date_end = ymd2date(e_time)

    h5 = LoadH5()

    data_dict = {}

    datetime_list = []
    for in_file in file_list:
        file_name = os.path.basename(in_file)
        if 'L3' in pair:
            list1 = ['aqua_chl1', 'aqua_kd490',  'aqua_poc', 'aqua_zsd']
            list2 = ['fy_chl1', 'fy_kd490',  'fy_poc', 'fy_zsd']
            reg = '.*_(\d{8}).*.HDF'
            m = re.match(reg, file_name)
            ymd = m.group(1)
            datetime_list.append(ymd2date(ymd))
        elif 'L2' in pair:
            list1 = ['aqua_chl1', 'aqua_kd490',  'aqua_poc',
                     'aqua_rw412', 'aqua_rw443', 'aqua_a490', 'aqua_rw490']
            list2 = ['fy_chl1', 'fy_kd490',  'fy_poc',
                     'aqua_rw412', 'fy_rw443', 'aqua_a490', 'aqua_rw490']
            reg = '.*_(\d{14}).*.HDF'
            m = re.match(reg, file_name)
            ymd = m.group(1)
            ymdhms = datetime.strptime('%s' % (ymd), "%Y%m%d%H%M%S")
            datetime_list.append(ymdhms)

        h5.load(in_file, ymd)

    # 在配置中
    dic_range = {'aqua_chl1': [-0.05, 4.00], 'fy_chl1': [-0.05, 4.00],
                 'aqua_kd490': [-0.001, 1.00], 'fy_kd490': [-0.001, 1.00],
                 'aqua_poc': [-0.05, 50.00], 'fy_poc': [-0.05, 50.00],
                 'aqua_zsd': [-0.05, 50.00], 'fy_zsd': [-0.05, 50.00],
                 'aqua_a490': [-0.05, 50.00], 'fy_a490': [-0.05, 50.00],
                 'aqua_rw412': [-0.05, 50.00], 'fy_rw412': [-0.05, 50.00],
                 'aqua_rw443': [-0.05, 50.00], 'fy_rw443': [-0.05, 50.00],
                 'aqua_rw490': [-0.05, 50.00], 'fy_rw490': [-0.05, 50.00],
                 'dif_y_range': [-0.3, 0.3]
                 }

    # 绘制每个数据集长时间
    for key in h5.data.keys():
        if key in ['Longitude', 'ymd', 'Latitude']:
            continue
        file_name = '%s_%s_%s_%s_time_series.png' % (pair, key, s_time, e_time)
        out_file_png = os.path.join(out_path, file_name)
        y_data = h5.data[key]

        plot_time_series(day_data_x=datetime_list, day_data_y=y_data,
                         y_range=dic_range[key],
                         out_file=out_file_png,
                         title='time_series %s %s ' % (pair, key), y_label=key,
                         ymd_start=s_time, ymd_end=e_time)
    # 绘制差值长时间序列图

    for key1, key2 in zip(list1, list2):
        print 'bias---'
        file_name = '%s_diff_%s_%s_%s_time_series.png' % (
            pair, key1.split('_')[1], s_time, e_time)
        out_file_png = os.path.join(out_path, file_name)
        y_data = h5.data[key]
        diff_data = h5.data[key1] - h5.data[key2]
        plot_time_series(day_data_x=datetime_list, day_data_y=diff_data,
                         y_range=dic_range['dif_y_range'],
                         out_file=out_file_png,
                         title='time_series %s %s %s' % (pair, key1, key2),
                         y_label='%s-%s' % (key1, key2),
                         ymd_start=s_time, ymd_end=e_time)

#     if len(chl1_list) >= 2:
#         time_list = []
#         value_aqua = []
#         value_fy = []
#         value_dif = []
#         y_range = [-0.05, 4.00]
#         dif_y_range = [-0.3, 0.3]
#         for file in chl1_list:
#             print file
#             time = ymd2date(os.path.basename(file).split("_")[1])
#             time_list.append(time)
#             try:
#                 h5file_r = h5py.File(file, 'r')
#                 aqua_chl1 = np.mean(h5file_r.get('aqua_chl1')[:])
#                 fy_chl1 = np.mean(h5file_r.get('fy_chl1')[:])
#                 dif_value_aqua_fy = np.mean(h5file_r.get('aqua_chl1')[
#                                             :] - h5file_r.get('fy_chl1')[:])
#                 value_aqua.append(aqua_chl1)
#                 value_fy.append(fy_chl1)
#                 value_dif.append(dif_value_aqua_fy)
#             except Exception as e:
#                 print str(e)
#                 return
#             finally:
#                 h5file_r.close()
#         out_file1 = out_path + os.sep + 'CHL1' + os.sep + \
#             '%s_%s_aqua_chl1——time_series.png' % (s_time, e_time)
#         out_file2 = out_path + os.sep + 'CHL1' + os.sep + \
#             '%s_%s_fy_chl1——time_series.png' % (s_time, e_time)
#         out_file3 = out_path + os.sep + 'CHL1' + os.sep + \
#             '%s_%s_dif_aqua-fy_chl1——time_series.png' % (s_time, e_time)
#         plot_time_series(day_data_x=time_list, day_data_y=value_aqua,
#                          y_range=y_range,
#                          out_file=out_file1,
#                          title='%s_%s_aqua_chl1——time_series' % (s_time, e_time), y_label='aqua_chl1',
#                          ymd_start=s_time, ymd_end=e_time, )
#         plot_time_series(day_data_x=time_list, day_data_y=value_fy,
#                          y_range=y_range,
#                          out_file=out_file2,
#                          title='%s_%s_fy_chl1——time_series' % (s_time, e_time), y_label='fy_chl1',
#                          ymd_start=s_time, ymd_end=e_time, )
#         plot_time_series(day_data_x=time_list, day_data_y=value_dif,
#                          y_range=dif_y_range,
#                          out_file=out_file3,
#                          title='%s_%s_dif_aqua-fy_chl1——time_series' % (s_time, e_time), y_label='dif_aqua-fy_chl1',
#                          ymd_start=s_time, ymd_end=e_time,
#                          zero_line=False)
#
#     if len(kd490_list) >= 2:
#         time_list = []
#         value_aqua = []
#         value_fy = []
#         value_dif = []
#         y_range = [-0.001, 1.00]
#         dif_y_range = [-0.3, 0.3]
#         for file in kd490_list:
#             print file
#             time = ymd2date(os.path.basename(file).split("_")[1])
#             time_list.append(time)
#             try:
#                 h5file_r = h5py.File(file, 'r')
#                 aqua_kd490 = np.mean(h5file_r.get('aqua_kd490')[:])
#                 fy_kd490 = np.mean(h5file_r.get('fy_kd490')[:])
#                 dif_value_aqua_fy = np.mean(h5file_r.get('aqua_kd490')[
#                                             :] - h5file_r.get('fy_kd490')[:])
#                 value_aqua.append(aqua_kd490)
#                 value_fy.append(fy_kd490)
#                 value_dif.append(dif_value_aqua_fy)
#             except Exception as e:
#                 print str(e)
#                 return
#             finally:
#                 h5file_r.close()
#         out_file1 = out_path + os.sep + 'KD490' + os.sep + \
#             '%s_%s_aqua_kd490——time_series.png' % (s_time, e_time)
#         out_file2 = out_path + os.sep + 'KD490' + os.sep + \
#             '%s_%s_fy_kd490——time_series.png' % (s_time, e_time)
#         out_file3 = out_path + os.sep + 'KD490' + os.sep + \
#             '%s_%s_dif_aqua-fy_kd490——time_series.png' % (s_time, e_time)
#         plot_time_series(day_data_x=time_list, day_data_y=value_aqua,
#                          y_range=y_range,
#                          out_file=out_file1,
#                          title='%s_%s_aqua_kd490——time_series' % (s_time, e_time), y_label='aqua_kd490',
#                          ymd_start=s_time, ymd_end=e_time, )
#         plot_time_series(day_data_x=time_list, day_data_y=value_fy,
#                          y_range=y_range,
#                          out_file=out_file2,
#                          title='%s_%s_fy_kd490——time_series' % (s_time, e_time), y_label='fy_kd490',
#                          ymd_start=s_time, ymd_end=e_time, )
#         plot_time_series(day_data_x=time_list, day_data_y=value_dif,
#                          y_range=dif_y_range,
#                          out_file=out_file3,
#                          title='%s_%s_dif_aqua-fy_kd490——time_series' % (s_time, e_time), y_label='dif_aqua-fy_kd490',
#                          ymd_start=s_time, ymd_end=e_time,
#                          zero_line=False)
#
#     if len(poc_list) >= 2:
#         time_list = []
#         value_aqua = []
#         value_fy = []
#         value_dif = []
#         y_range = [-0.05, 50.00]
#         dif_y_range = [-0.3, 0.3]
#         for file in poc_list:
#             print file
#             time = ymd2date(os.path.basename(file).split("_")[1])
#             time_list.append(time)
#             try:
#                 h5file_r = h5py.File(file, 'r')
#                 aqua_poc = np.mean(h5file_r.get('aqua_poc')[:])
#                 fy_poc = np.mean(h5file_r.get('fy_poc')[:])
#                 dif_value_aqua_fy = np.mean(h5file_r.get('aqua_poc')[
#                                             :] - h5file_r.get('fy_poc')[:])
#                 value_aqua.append(aqua_poc)
#                 value_fy.append(fy_poc)
#                 value_dif.append(dif_value_aqua_fy)
#             except Exception as e:
#                 print str(e)
#                 return
#             finally:
#                 h5file_r.close()
#         out_file1 = out_path + os.sep + 'POC' + os.sep + \
#             '%s_%s_aqua_poc——time_series.png' % (s_time, e_time)
#         out_file2 = out_path + os.sep + 'POC' + os.sep + \
#             '%s_%s_fy_poc——time_series.png' % (s_time, e_time)
#         out_file3 = out_path + os.sep + 'POC' + os.sep + \
#             '%s_%s_dif_aqua-fy_poc——time_series.png' % (s_time, e_time)
#         plot_time_series(day_data_x=time_list, day_data_y=value_aqua,
#                          y_range=y_range,
#                          out_file=out_file1,
#                          title='%s_%s_aqua_poc——time_series' % (s_time, e_time), y_label='aqua_poc',
#                          ymd_start=s_time, ymd_end=e_time, )
#         plot_time_series(day_data_x=time_list, day_data_y=value_fy,
#                          y_range=y_range,
#                          out_file=out_file2,
#                          title='%s_%s_fy_poc——time_series' % (s_time, e_time), y_label='fy_poc',
#                          ymd_start=s_time, ymd_end=e_time, )
#         plot_time_series(day_data_x=time_list, day_data_y=value_dif,
#                          y_range=dif_y_range,
#                          out_file=out_file3,
#                          title='%s_%s_dif_aqua-fy_poc——time_series' % (s_time, e_time), y_label='dif_aqua-fy_poc',
#                          ymd_start=s_time, ymd_end=e_time,
#                          zero_line=False)
#     if len(zsd_list) >= 2:
#         time_list = []
#         value_aqua = []
#         value_fy = []
#         value_dif = []
#         y_range = [-0.05, 50.00]
#         dif_y_range = [-0.3, 0.3]
#         for file in zsd_list:
#             print file
#             time = ymd2date(os.path.basename(file).split("_")[1])
#             time_list.append(time)
#             try:
#                 h5file_r = h5py.File(file, 'r')
#                 aqua_zsd = np.mean(h5file_r.get('aqua_zsd')[:])
#                 fy_zsd = np.mean(h5file_r.get('fy_zsd')[:])
#                 dif_value_aqua_fy = np.mean(h5file_r.get('aqua_zsd')[
#                                             :] - h5file_r.get('fy_zsd')[:])
#                 value_aqua.append(aqua_zsd)
#                 value_fy.append(fy_zsd)
#                 value_dif.append(dif_value_aqua_fy)
#             except Exception as e:
#                 print str(e)
#                 return
#             finally:
#                 h5file_r.close()
#         out_file1 = out_path + os.sep + 'ZSD' + os.sep + \
#             '%s_%s_aqua_zsd——time_series.png' % (s_time, e_time)
#         out_file2 = out_path + os.sep + 'ZSD' + os.sep + \
#             '%s_%s_fy_zsd——time_series.png' % (s_time, e_time)
#         out_file3 = out_path + os.sep + 'ZSD' + os.sep + \
#             '%s_%s_dif_aqua-fy_zsd——time_series.png' % (s_time, e_time)
#         plot_time_series(day_data_x=time_list, day_data_y=value_aqua,
#                          y_range=y_range,
#                          out_file=out_file1,
#                          title='%s_%s_aqua_zsd——time_series' % (s_time, e_time), y_label='aqua_zsd',
#                          ymd_start=s_time, ymd_end=e_time, )
#         plot_time_series(day_data_x=time_list, day_data_y=value_fy,
#                          y_range=y_range,
#                          out_file=out_file2,
#                          title='%s_%s_fy_zsd——time_series' % (s_time, e_time), y_label='fy_zsd',
#                          ymd_start=s_time, ymd_end=e_time, )
#         plot_time_series(day_data_x=time_list, day_data_y=value_dif,
#                          y_range=dif_y_range,
#                          out_file=out_file3,
#                          title='%s_%s_dif_aqua-fy_zsd——time_series' % (s_time, e_time), y_label='dif_aqua-fy_zsd',
#                          ymd_start=s_time, ymd_end=e_time,
#                          zero_line=False)


def get_one_day_files(all_files, ymd, ext=None, pattern_ymd=None):
    """
    :param all_files: 文件列表
    :param ymd:
    :param ext: 后缀名, '.hdf5'
    :param pattern_ymd: 匹配时间的模式, 可以是 r".*(\d{8})_(\d{4})_"
    :return: list
    """
    files_found = []
    if pattern_ymd is not None:
        pattern = pattern_ymd
    else:
        pattern = r".*(\d{8})"

    for file_name in all_files:
        if ext is not None:
            if '.' not in ext:
                ext = '.' + ext
            if os.path.splitext(file_name)[1].lower() != ext.lower():
                continue
        re_result = re.match(pattern, file_name)
        if re_result is not None:
            time_file = ''.join(re_result.groups())
        else:
            continue
        if int(time_file) == int(ymd):
            files_found.append(os.path.join(file_name))
    return files_found


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

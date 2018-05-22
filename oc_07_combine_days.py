# coding:utf-8

import os
import sys

import h5py
import yaml
import numpy as np
from configobj import ConfigObj

from PB import pb_io
from PB.pb_time import time_block
from PB.pb_space import deg2meter
from PB.CSC.pb_csc_console import LogServer


TIME_TEST = True  # 时间测试


def run(sat_sensor, yaml_file):
    ######################### 初始化 ###########################
    # 加载程序配置文件
    proj_cfg_file = os.path.join(main_path, "global.yaml")
    proj_cfg = pb_io.load_yaml_config(proj_cfg_file)
    if proj_cfg is None:
        log.error("File is not exist: {}".format(proj_cfg_file))
        return
    else:
        # 加载配置信息
        try:
            RES = proj_cfg['project'][sat_sensor]['res']
            half_res = deg2meter(RES) / 2.
            CMD = proj_cfg['project'][sat_sensor]['cmd'] % (half_res, half_res)
            ROW = proj_cfg['project'][sat_sensor]['row']
            COL = proj_cfg['project'][sat_sensor]['col']
            MESH_SIZE = proj_cfg['project'][sat_sensor]['mesh_zise']
            if pb_io.is_none(CMD, ROW, COL, RES, MESH_SIZE):
                log.error("Yaml args is not completion. : {}".format(proj_cfg_file))
                return
        except Exception as why:
            print why
            log.error("Load yaml config file error, please check it. : {}".format(proj_cfg_file))
            return

    ######################### 开始处理 ###########################
    # 判断 yaml 文件是否存在
    if not os.path.isfile(yaml_file):
        log.error("File is not exist: {}".format(yaml_file))
        return
    else:
        combine = Combine()  # 初始化一个投影实例
        combine.load_yaml(yaml_file)  # 加载 yaml 文件

        with time_block("One combine time:", switch=TIME_TEST):
            combine.combine()
        with time_block("One write time:", switch=TIME_TEST):
            combine.write()


class Combine(object):

    def __init__(self):
        self.error = False

        self.yaml_file = None

        self.ifile = None
        self.ofile = None

        self.in_data = {}
        self.attrs = {}
        self.out_data = {}
        self.counter = {}

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
            log.error("Load yaml file error, please check it. : {}".format(yaml_file))
            self.error = True

    def _data_calculate(self):
        """
        计算数据的平均值
        :return:
        """
        fill_value = -32767
        for k, counter in self.counter.items():
            idx = np.where(counter > 0)
            print k
            print idx
            print self.out_data[k][idx]
            print counter[idx]
            self.out_data[k][idx] = self.out_data[k][idx] / counter[idx]
            print self.out_data[k][idx]
            idx = np.less_equal(self.out_data[k], 0)
            self.out_data[k][idx] = fill_value

    def combine(self):
        if self.error:
            return

        # 如果输出文件已经存在，跳过
        elif os.path.isfile(self.ofile):
            log.error("File is already exist, skip it: {}".format(self.ofile))
            return
        # 合成日数据
        elif pb_io.is_none(self.ifile, self.ofile):
            self.error = True
            log.error("Is None: ifile or pfile or ofile: {}".format(self.yaml_file))
            return
        elif len(self.ifile) < 1:
            self.error = True
            log.error("File count lower than 1: {}".format(self.yaml_file))

        for in_file in self.ifile:
            if os.path.isfile(in_file):
                print "Start combining file: {}".format(in_file)
            else:
                log.error("File is not exist: {}".format(in_file))
                continue

            # 日合成
            with time_block("One combine time:", switch=TIME_TEST):
                try:
                    with h5py.File(in_file, 'r') as h5:
                        for k in h5.keys():
                            if k not in self.out_data.keys():  # 创建输出数据集和计数器
                                shape = h5.get(k).shape
                                if k == "Ocean_Flag":
                                    self.out_data[k] = np.full(shape, 0, dtype='i4')
                                elif k == "Longitude" or k == "Latitude":
                                    self.out_data[k] = h5.get(k)[:]
                                else:
                                    self.out_data[k] = np.full(shape, 0, dtype='i2')
                                    self.counter[k] = np.full(shape, 0, dtype='i2')
                            if k == "Longitude" or k == "Latitude":
                                continue
                            elif k == "Ocean_Flag":
                                value = h5.get(k)[:]
                                idx = np.where(value > 0)  # TODO 判断保留那些值，或者哪些值可以覆盖其他值
                                self.out_data[k][idx] = value[idx]
                            else:
                                print '*' * 20
                                print k
                                value = h5.get(k)[:]
                                idx_test = np.logical_and(self.out_data[k] > 0, value > 0)
                                idx_test = np.where(idx_test)
                                print idx_test
                                if len(idx_test[0]) != 0:
                                    print idx_test[0][0], idx_test[1][0]
                                    print self.out_data[k][idx_test[0][0], idx_test[1][0]]
                                    print value[idx_test[0][0], idx_test[1][0]]
                                print '*' * 20
                                print '-' * 20
                                idx = np.where(value > 0)
                                print k
                                print self.out_data[k][idx]
                                print value[idx]
                                self.out_data[k][idx] = self.out_data[k][idx] + value[idx]
                                print self.out_data[k][idx]
                                print self.counter[k][idx]
                                self.counter[k][idx] += 1
                                print self.counter[k][idx]
                                print '-' * 20

                            # 记录属性信息
                            if k not in self.attrs.keys():
                                self.attrs[k] = pb_io.attrs2dict(h5.get(k).attrs)
                    print '-' * 100

                except Exception as why:
                    print why
                    print "Can't combine file, some error exist: {}".format(in_file)

        # 计算数据的平均值
        with time_block("Calculate mean time:"):
            self._data_calculate()

        # 输出数据集有效数据的数量
        for k, v in self.out_data.items():
            idx = np.where(v > 0)
            print len(idx[0]), k

    def write(self):
        if self.error:
            return
        pb_io.make_sure_path_exists(os.path.dirname(self.ofile))
        # 写入 HDF5 文件
        with h5py.File(self.ofile, 'w') as h5:
            for k in self.out_data.keys():
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
                if k == "Longitude" or k == "Latitude":
                    continue
                attrs = self.attrs[k]
                for key, value in attrs.items():
                    h5[k].attrs[key] = value
        print "Output file: {}".format(self.ofile)


######################### 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    args = sys.argv[1:]
    help_info = \
        """
        [参数1]：合成配置文件
        [样例]： python 程序 合成配置文件
        """
    if "-h" in args:
        print help_info
        sys.exit(-1)

    # 获取程序所在位置，拼接配置文件
    main_path, main_file = os.path.split(os.path.realpath(__file__))
    project_path = main_path
    config_file = os.path.join(project_path, "global.cfg")

    # 配置不存在预警
    if not os.path.isfile(config_file):
        print ("配置文件不存在 %s" % config_file)
        sys.exit(-1)

    # 载入配置文件
    inCfg = ConfigObj(config_file)
    LOG_PATH = inCfg["PATH"]["OUT"]["log"]
    log = LogServer(LOG_PATH)

    # 开启进程池
    # thread_number = inCfg["CROND"]["threads"]
    # thread_number = 1
    # pool = Pool(processes=int(thread_number))

    if not len(args) == 1:
        print help_info
    else:
        FILE_PATH = args[0]
        SAT = inCfg["PATH"]["sat"]
        SENSOR = inCfg["PATH"]["sensor"]
        SAT_SENSOR = "{}+{}".format(SAT, SENSOR)

        with time_block("All combine time:", switch=TIME_TEST):
            run(SAT_SENSOR, FILE_PATH)
        # pool.apply_async(run, (sat_sensor, file_path))
        # pool.close()
        # pool.join()

# coding:utf-8

import os
import sys
import calendar
from datetime import datetime
from multiprocessing import Pool, Lock

import numpy as np
import h5py
from matplotlib.ticker import MultipleLocator

from configobj import ConfigObj
from dateutil.relativedelta import relativedelta
from numpy.lib.polynomial import polyfit
from numpy.ma.core import std, mean
from numpy.ma.extras import corrcoef

from PB.CSC.pb_csc_console import LogServer
from PB import pb_time, pb_io
from ocrs_io import loadYamlCfg

from DV.dv_img import dv_rgb

import matplotlib as mpl
mpl.use("agg")
import matplotlib.pyplot as plt


def run(pair, hdf5_file):
    if os.path.isfile(hdf5_file):
        log.error("hdf5 file not exist： ".format(hdf5_file))
        return

    # 加载程序配置文件
    proj_cfg_file = os.path.join(main_path, "cfg", "global.yaml")
    proj_cfg = loadYamlCfg(proj_cfg_file)
    if proj_cfg is None:
        log.error("Not find the config file: {}".format(proj_cfg_file))
        return

    # 加载配置信息
    try:
        datasets = proj_cfg["plt_gray"][pair].get("datasets")
        filename_suffix = proj_cfg["plt_gray"][pair].get("filename_suffix")
    except ValueError:
        log.error("Please check the yaml plt_gray args")
        return

    # 开始绘图 ------------------------------------------------------
    log.info("start plot gray picture")
    file_name = os.path.splitext(hdf5_file)[0]
    out_pic = "{}_{}.{}".format(file_name, filename_suffix, "png")

    with h5py.File(hdf5_file, 'r') as h5:
        if len(datasets) == 3:
            datas = []
            for set_name in datasets:
                datas.append(h5.get(set_name)[:])
            dv_rgb(datas[0], datas[1], datas[2], out_pic)
            log.info(out_pic)
        elif len(datasets) == 1:
            for set_name in datasets:
                data = h5.get(set_name)
                plt.imshow(data, cmap=plt.cm.gray)
                plt.savefig(out_pic, dpi=200)
                log.info(out_pic)
        else:
            log.error("datasets must be 1 or 3")


######################### 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    args = sys.argv[1:]
    help_info = \
        u"""
        [参数1]：SAT1+SENSOR1
        [参数2]：文件路径
        """
    if "-h" in args:
        print help_info
        sys.exit(-1)

    # 获取程序所在位置，拼接配置文件
    main_path, main_file = os.path.split(os.path.realpath(__file__))
    project_path = main_path
    config_file = os.path.join(project_path, "cfg", "global.cfg")

    # 配置不存在预警
    if not os.path.isfile(config_file):
        print (u"配置文件不存在 %s" % config_file)
        sys.exit(-1)

    # 载入配置文件
    inCfg = ConfigObj(config_file)
    LOG_PATH = inCfg["PATH"]["OUT"]["LOG"]
    log = LogServer(LOG_PATH)

    # 开启进程池
    thread_number = inCfg["CROND"]["threads"]
    # thread_number = 1
    pool = Pool(processes=int(thread_number))

    if not len(args) == 2:
        print help_info

    else:
        # file_path = "/storage-space/disk3/admin/product/FY3B/973Aerosol/Granule/2013/201310/20131012/20131012_0025_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20131012_0025_1000M.HDF"
        # file_path = "/storage-space/disk3/Granule/out/2017/201710/20171012/20171012_0000_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20171012_0000_1000M.HDF"
        # pair_tem = "FY3B+MERSI"
        # run(pair_tem, file_path)
        sat_sensor = args[0]
        file_path = args[1]
        run(sat_sensor, file_path)
        # pool.apply_async(run, (sat_sensor, file_path))

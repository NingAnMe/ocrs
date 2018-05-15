# coding:utf-8

import os
import sys
import calendar
from datetime import datetime
from multiprocessing import Pool, Manager

import numpy as np
import h5py
import yaml
from matplotlib.ticker import MultipleLocator

from configobj import ConfigObj
from dateutil.relativedelta import relativedelta
from numpy.lib.polynomial import polyfit
from numpy.ma.core import std, mean
from numpy.ma.extras import corrcoef

from PB.CSC.pb_csc_console import LogServer
from DP.dp_prj_new import prj_core
from DV import dv_map
from PB import pb_time, pb_io
from PB.pb_space import deg2meter
from ocrs_io import loadYamlCfg
from publicmodels.pm_time import time_block


def run(pair, in_file):
    # 加载程序配置文件
    proj_cfg_file = os.path.join(main_path, "cfg", "global.yaml")
    proj_cfg = loadYamlCfg(proj_cfg_file)
    if proj_cfg is None:
        log.error("Can't find the config file: {}".format(proj_cfg_file))
        return

    legend_range = proj_cfg["plt_combine"][pair].get("legend_range")
    area_range = proj_cfg["plt_combine"][pair].get("area_range")

    if os.path.isfile(in_file):
        print "Start draw combine: {}".format(in_file)
    else:
        print "File is not exist: {}".format(in_file)
        return

    for legend in legend_range:
        dataset_name = legend[0]  # 数据集名称
        vmax = float(legend[1])  # color bar 范围 最大值
        vmin = float(legend[2])  # color bar 范围 最小值
        dir_path = os.path.dirname(in_file)
        pic_name = os.path.join(dir_path, "pic/{}_{}_AOAD.png".format(pair, dataset_name))
        with time_block("draw combine"):
            draw_combine(in_file, dataset_name, pic_name, vmin=vmin, vmax=vmax, area_range=area_range)


def draw_combine(in_file, dataset_name, pic_name, vmin=None, vmax=None, area_range=None):
    """
    通过日合成文件，画数据集的全球分布图
    :param in_file:
    :param dataset_name:
    :param pic_name:
    :param vmin:
    :param vmax:
    :return:
    """
    try:
        with h5py.File(in_file, 'r') as h5:
            value = h5.get(dataset_name)[:]
            lats = h5.get("Latitude")[:]
            lons = h5.get("Longitude")[:]
    except Exception as why:
        print why
        return

    idx = np.where(value >= 0)
    if len(idx[0]) == 0:
        print "Don't have enough valid value： {}  {}".format(dataset_name, len(idx[0]))
        return
    else:
        print "{} valid value count: {}".format(dataset_name, len(idx[0]))

    p = dv_map.dv_map()
    p.colorbar_fmt = "%0.2f"
    p.title = dataset_name
    out_png = pic_name
    # 增加省边界
    #       p1.show_china_province = True
    p.delat = 30
    p.delon = 30
    p.show_line_of_latlon = False

    # 是否绘制某个区域
    if area_range is None:
        box = None
    else:
        lat_n = area_range.get("lat_n")
        lat_s = area_range.get("lat_s")
        lon_w = area_range.get("lon_w")
        lon_e = area_range.get("lon_e")
        box = [lat_s, lat_n, lon_w, lon_e]

    value = np.ma.masked_less(value, 0)  # 掩去无效值

    slope = 0.001
    value = value * slope  # 值要乘 slope 以后使用，slope 为 0.001

    p.easyplot(lats, lons, value, ptype=None, vmin=vmin, vmax=vmax, box=box, markersize=0.05, marker='o')
    pb_io.make_sure_path_exists(os.path.dirname(out_png))
    p.savefig(out_png, dpi=300)
    print out_png


######################### 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    args = sys.argv[1:]
    help_info = \
        u"""
        [参数1]：SAT1+SENSOR1
        [参数2]：file_path
        """
    if "-h" in args:
        print help_info
        sys.exit(-1)

    # 获取程序所在位置，拼接全局配置文件
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
        sat_sensor = args[0]
        file_path = args[1]
        file_path = '/storage-space/disk3/Granule/out_del_cloudmask/2017/201710/20171012/20171012_0000_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20171012_0000_1000M_COMBINE.HDF'
        run(sat_sensor, file_path)
        # pool.apply_async(run, (sat_sensor, file_path))
        # pool.close()
        # pool.join()

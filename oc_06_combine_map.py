# coding:utf-8

import os
import sys

import h5py
import numpy as np
from configobj import ConfigObj

from DV import dv_map
from PB import pb_io
from PB.pb_time import time_block
from PB.CSC.pb_csc_console import LogServer


TIME_TEST = True  # 时间测试


def run(sat_sensor, in_file):
    ######################### 初始化 ###########################
    # 加载程序配置文件
    proj_cfg_file = os.path.join(main_path, "global.yaml")
    proj_cfg = pb_io.load_yaml_config(proj_cfg_file)
    if proj_cfg is None:
        log.error("Can't find the config file: {}".format(proj_cfg_file))
        return
    else:
        # 加载配置信息
        try:
            LEGEND_RANGE = proj_cfg["plt_combine"][sat_sensor].get("legend_range")
            AREA_RANGE = proj_cfg["plt_combine"][sat_sensor].get("area_range")
            if pb_io.is_none(LEGEND_RANGE, AREA_RANGE):
                log.error("Yaml args is not completion. : {}".format(proj_cfg_file))
                return
            for k, v in AREA_RANGE.items():
                AREA_RANGE[k] = float(v)
        except Exception as why:
            print why
            log.error("Please check the yaml plt_gray args")
            return

    ######################### 开始处理 ###########################
    print '-' * 100

    if os.path.isfile(in_file):
        print "Start draw combine picture: {}".format(in_file)
    else:
        log.error("File is not exist: {}".format(in_file))
        return

    for legend in LEGEND_RANGE:
        dataset_name = legend[0]  # 数据集名称
        vmax = float(legend[1])  # color bar 范围 最大值
        vmin = float(legend[2])  # color bar 范围 最小值
        dir_path = os.path.dirname(in_file)
        pic_name = os.path.join(dir_path, "pictures/{}_{}_AOAD.png".format(sat_sensor, dataset_name))
        with time_block("Draw combine time:", switch=TIME_TEST):
            draw_combine(in_file, dataset_name, pic_name, vmin=vmin, vmax=vmax, area_range=AREA_RANGE)

    print '-' * 100


def draw_combine(in_file, dataset_name, pic_name, vmin=None, vmax=None, area_range=None):
    """
    通过日合成文件，画数据集的全球分布图
    :param area_range:
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

    # # 测试C程序的产品的时候，没有经纬度数据集，需要 Python 产品某个文件的经纬度数据集
    # file_path = '/storage-space/disk3/Granule/out_del_cloudmask/2013/201301/20130101/20130101_0000_1000M/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20130101_0000_1000M_COMBINE.HDF'
    # try:
    #     with h5py.File(file_path, 'r') as h5:
    #         # value = h5.get(dataset_name)[:]
    #         lats = h5.get("Latitude")[:]
    #         lons = h5.get("Longitude")[:]
    # except Exception as why:
    #     print why
    #     return

    idx = np.where(value > 0)  # 不计算小于 0 的无效值
    if len(idx[0]) == 0:
        print "Don't have enough valid value： {}  {}".format(dataset_name, len(idx[0]))
        return
    else:
        print "{} valid value count: {}".format(dataset_name, len(idx[0]))

    value = np.ma.masked_less_equal(value, 0)  # 掩去小于等于 0 的无效值

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
        lat_n = float(area_range.get("lat_n"))
        lat_s = float(area_range.get("lat_s"))
        lon_w = float(area_range.get("lon_w"))
        lon_e = float(area_range.get("lon_e"))
        box = [lat_s, lat_n, lon_w, lon_e]

    slope = 0.001
    value = value * slope  # 值要乘 slope 以后使用，slope 为 0.001

    p.easyplot(lats, lons, value, ptype=None, vmin=vmin, vmax=vmax, box=box, markersize=0.05, marker='o')
    pb_io.make_sure_path_exists(os.path.dirname(out_png))
    p.savefig(out_png, dpi=300)
    print "Output picture: {}".format(out_png)


######################### 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    args = sys.argv[1:]
    help_info = \
        u"""
        [参数1]：HDF5文件
        [样例]：python 程序 HDF5文件
        """
    if "-h" in args:
        print help_info
        sys.exit(-1)

    # 获取程序所在位置，拼接全局配置文件
    main_path, main_file = os.path.split(os.path.realpath(__file__))
    project_path = main_path
    config_file = os.path.join(project_path, "global.cfg")

    # 配置不存在预警
    if not os.path.isfile(config_file):
        print (u"配置文件不存在 %s" % config_file)
        sys.exit(-1)

    # 载入配置文件
    inCfg = ConfigObj(config_file)
    LOG_PATH = inCfg["PATH"]["OUT"]["log"]
    log = LogServer(LOG_PATH)

    # 开启进程池
    # thread_number = inCfg["CROND"]["threads"]
    # thread_number = 1
    # pool = Pool(processes=int(thread_number))

    if not len(args) == 2:
        print help_info
    else:
        FILE_PATH = args[0]
        SAT = inCfg["PATH"]["sat"]
        SENSOR = inCfg["PATH"]["sensor"]
        SAT_SENSOR = "{}+{}".format(SAT, SENSOR)

        with time_block("Plot combine map time:"):
            run(SAT_SENSOR, FILE_PATH)
        # pool.apply_async(run, (sat_sensor, file_path))
        # pool.close()
        # pool.join()

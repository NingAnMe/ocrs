# coding:utf-8
"""
水色产品的全球分布图绘制
~~~~~~~~~~~~~~~~~~~
creation time : 2018 5 16
author : anning
~~~~~~~~~~~~~~~~~~~
"""
import os
import re
import sys

import h5py
import numpy as np
from configobj import ConfigObj

from DV import dv_map
from PB import pb_io, pb_time
from PB.pb_time import time_block
from PB.CSC.pb_csc_console import LogServer


TIME_TEST = True  # 时间测试


def main(sat_sensor, in_file):
    # ######################## 初始化 ###########################
    # 加载程序配置文件
    proj_cfg_file = os.path.join(MAIN_PATH, "global.yaml")
    proj_cfg = pb_io.load_yaml_config(proj_cfg_file)
    if proj_cfg is None:
        LOG.error("Can't find the config file: {}".format(proj_cfg_file))
        return
    else:
        # 加载配置信息
        try:
            LEGEND_RANGE = proj_cfg["plt_combine"][sat_sensor].get("colorbar_range")
            AREA_RANGE = proj_cfg["plt_combine"][sat_sensor].get("area_range")
            if pb_io.is_none(LEGEND_RANGE, AREA_RANGE):
                LOG.error("Yaml args is not completion. : {}".format(proj_cfg_file))
                return
            for k, v in AREA_RANGE.items():
                AREA_RANGE[k] = float(v)
        except Exception as why:
            print why
            LOG.error("Please check the yaml plt_gray args")
            return

    # ######################## 开始处理 ###########################
    print '-' * 100

    if os.path.isfile(in_file):
        print "Start draw combine picture: {}".format(in_file)
    else:
        LOG.error("File is not exist: {}".format(in_file))
        return

    for legend in LEGEND_RANGE:
        dataset_name = legend[0]  # 数据集名称
        vmax = float(legend[1])  # color bar 范围 最大值
        vmin = float(legend[2])  # color bar 范围 最小值
        dir_path = os.path.dirname(in_file)
        ymd = pb_time.get_ymd(in_file)
        kind = get_kind(in_file)
        pic_name = os.path.join(dir_path, "pictures/{}_{}_{}_{}.png".format(
            sat_sensor, dataset_name, ymd, kind))

        # 如果输出文件已经存在，跳过
        # if os.path.isfile(pic_name):
        #     print "File is already exist, skip it: {}".format(pic_name)
        #     continue

        with time_block("Draw combine time:", switch=TIME_TEST):
            draw_combine(in_file, dataset_name, pic_name, vmin=vmin, vmax=vmax,
                         area_range=AREA_RANGE)

    print '-' * 100


def get_kind(l3_file_name):
    """
    获取 L3 产品中的合成种类名称
    """
    m = re.match(r".*_\d{4,8}_(\w{4})_", l3_file_name)
    try:
        kind = m.groups()[0]
    except Exception as why:
        print why
        kind = "AOAD"
    return kind


def draw_combine(in_file, dataset_name, pic_name, vmin=None, vmax=None, area_range=None):
    """
    通过日合成文件，画数据集的全球分布图
    文件中需要有 Latitude 和Longitude 两个数据集
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
            dataset = h5.get(dataset_name)

            value = dataset[:]
            slope = dataset.attrs["Slope"]
            intercept = dataset.attrs["Intercept"]
            value = value * slope + intercept

            lats = h5.get("Latitude")[:]
            lons = h5.get("Longitude")[:]
    except Exception as why:
        print why
        return

    # 过滤有效范围外的值
    idx = np.where(value > 0)  # 不计算小于 0 的无效值
    if len(idx[0]) == 0:
        print "Don't have enough valid value： {}  {}".format(dataset_name, len(idx[0]))
        return
    else:
        print "{} valid value count: {}".format(dataset_name, len(idx[0]))

    value = value[idx]
    lats = lats[idx]
    lons = lons[idx]

    log_set = ["Ocean_CHL1", "Ocean_CHL2", "Ocean_PIG1", "Ocean_TSM", "Ocean_YS443", ]
    if dataset_name in log_set:
        print "-" * 100
        print dataset_name
        print value.min()
        print value.max()
        value = np.log10(value)
        d = np.histogram(value, bins=[x * 0.05 for x in xrange(-40, 80)])
        for i in xrange(len(d[0])):
            print "{:10} :: {:10}".format(d[1][i], d[0][i])
        print value.min()
        print value.max()
        print "-" * 100

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
        lat_s = float(area_range.get("lat_s"))
        lat_n = float(area_range.get("lat_n"))
        lon_w = float(area_range.get("lon_w"))
        lon_e = float(area_range.get("lon_e"))
        box = [lat_s, lat_n, lon_w, lon_e]

    p.easyplot(lats, lons, value, ptype=None, vmin=vmin, vmax=vmax, box=box, markersize=0.05,
               marker='o')
    pb_io.make_sure_path_exists(os.path.dirname(out_png))
    p.savefig(out_png, dpi=300)
    print "Output picture: {}".format(out_png)


# ######################## 程序全局入口 ############################# #
if __name__ == "__main__":
    # 获取程序参数接口
    ARGS = sys.argv[1:]
    HELP_INFO = \
        u"""
        [arg1]：hdf_file
        [example]： python app.py arg1
        """
    if "-h" in ARGS:
        print HELP_INFO
        sys.exit(-1)

    # 获取程序所在位置，拼接配置文件
    MAIN_PATH = os.path.dirname(os.path.realpath(__file__))
    CONFIG_FILE = os.path.join(MAIN_PATH, "global.cfg")

    # 配置不存在预警
    if not os.path.isfile(CONFIG_FILE):
        print "File is not exist: {}".format(CONFIG_FILE)
        sys.exit(-1)

    # 载入配置文件
    IN_CFG = ConfigObj(CONFIG_FILE)
    LOG_PATH = IN_CFG["PATH"]["OUT"]["log"]
    LOG = LogServer(LOG_PATH)

    if not len(ARGS) == 1:
        print HELP_INFO
    else:
        FILE_PATH = ARGS[0]
        SAT = IN_CFG["PATH"]["sat"]
        SENSOR = IN_CFG["PATH"]["sensor"]
        SAT_SENSOR = "{}+{}".format(SAT, SENSOR)

        with time_block("Plot combine map time:"):
            main(SAT_SENSOR, FILE_PATH)

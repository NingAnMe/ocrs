#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/12/24 10:17
@Author  : AnNing
"""
import os
import sys
import numpy as np

from PB.pb_time import time_block
from PB import pb_io
from app.read_data import ReadCrossData

from DV import dv_map_oc

TIME_TEST = False  # 时间测试
RED = '#f63240'
BLUE = '#1c56fb'
GRAY = '#c0c0c0'
EDGE_GRAY = '#303030'


L_DIR = '/aerosol/CMA_OCC/SupportData/MatchedData/FY3B+MERSI_AQUA+MODIS_{}'


def main(yyyymm, level):
    """
    :return:
    """

    # 加载业务配置信息
    # ######################## 开始处理 ###########################
    dir_file = os.path.join(L_DIR.format(level), yyyymm)

    files = os.listdir(dir_file)
    in_files = []
    for f in files:
        if '.H5' in f:
            in_files.append(f)

    for i in in_files:
        print i

    in_files.sort()

    for file_name in in_files:
        ymd = os.path.splitext(file_name)[0]
        in_file = os.path.join(dir_file, file_name)

        dir_pic = os.path.join(dir_file, ymd)

        # 加载数据
        cross_data = ReadCrossData()
        cross_data.read_cross_data(in_files=[in_file])

        # 获取绘图范围
        lat_s1 = cross_data.data['MERSI_Lats']
        lon_s1 = cross_data.data['MERSI_Lons']

        lat_s2 = cross_data.data['MODIS_Lats']
        lon_s2 = cross_data.data['MODIS_Lons']

        lat_all = np.append(lat_s1, lat_s2)
        lon_all = np.append(lon_s1, lon_s2)

        box = get_box(lat_all, lon_all)

        # 循环通道数据
        for channel in cross_data.data:

            if not isinstance(cross_data.data[channel], dict):
                continue
            for sensor in ['MERSI', 'MODIS']:

                ref_s1_all = cross_data.data[channel]['{}_FovMean'.format(sensor)]
                lat_s1_all = cross_data.data['{}_Lats'.format(sensor)]
                lon_s1_all = cross_data.data['{}_Lons'.format(sensor)]

                idx = np.where(ref_s1_all > 0)
                ref_s1_all = ref_s1_all[idx]
                lat_s1_all = lat_s1_all[idx]
                lon_s1_all = lon_s1_all[idx]

                if len(ref_s1_all) > 0:

                    # 以下物理量需要使用对数值
                    for i in ["Ocean_CHL1", "Ocean_CHL2", "Ocean_PIG1", "Ocean_TSM",
                              "Ocean_YS443"]:
                        if channel.lower() in i.lower():
                            ref_s1_all = np.log10(ref_s1_all)

                    print_statistics(ref_s1_all, channel, sensor)

                    filename1 = "{}_{}.png".format(channel, sensor)
                    out_file1 = os.path.join(dir_pic, filename1)
                    plot_map(lat_s1_all, lon_s1_all, ref_s1_all, out_file1, box=box)
                    print '>>> {}'.format(out_file1)


def plot_map(lats, lons, values, out_file, box=None):
    p = dv_map_oc.dv_map()
    p.show_bg_color = False
    p.colorbar_fmt = "%0.2f"

    title = ''

    # 是否绘制某个区域
    if box:
        lat_s = float(box[0])
        lat_n = float(box[1])
        lon_w = float(box[2])
        lon_e = float(box[3])
        box = [lat_s, lat_n, lon_w, lon_e]
    else:
        box = [90, -90, -180, 180]

    # 绘制经纬度线
    p.delat = 10
    p.delon = 10
    if box[0] - box[1] > 90:
        p.delat = 30  # 30
    elif box[2] - box[3] > 180:
        p.delon = 30  # 30
    p.show_line_of_latlon = True

    # 是否设置 colorbar 范围
    if len(values) == 0:
        return
    vmin = np.min(values)
    vmax = np.max(values)
    # # 是否填写 colorbar title
    # p.colorbar_label = colorbar_label
    #
    # p.colorbar_ticks = legend["ticks"]
    #
    # p.colorbar_tick_labels = legend["tick_labels"]

    p.title = title
    with time_block("plot combine map", switch=TIME_TEST):
        p.easyplot(lats, lons, values, ptype=None, vmin=vmin, vmax=vmax, box=box, markersize=2,
                   marker='o')
        # p.easyplot(lats, lons, value, ptype="pcolormesh", vmin=vmin, vmax=vmax, box=box)
        pb_io.make_sure_path_exists(os.path.dirname(out_file))
        p.savefig(out_file, dpi=300)
        print '>>> {}'.format(out_file)


def print_statistics(data, data_name, sensor):
    if len(data) == 0:
        print data_name
    else:
        print '{:15s}  min:{:.6f} max:{:.6f} mean：{:.6f}'.format(
            data_name+'/'+sensor, np.min(data), np.max(data), np.mean(data))


def get_box(lats, lons):
    lat_min = np.min(lats)
    lat_max = np.max(lats)

    lons_min = np.min(lons)
    lons_max = np.max(lons)

    lat_s = 90
    for i in xrange(-90, 91, 30):
        if i >= lat_max:
            lat_s = i
            break

    lat_n = -90
    for i in xrange(90, -91, -30):
        if i <= lat_min:
            lat_n = i
            break

    lon_w = -180
    for i in xrange(180, -181, -30):
        if i <= lons_min:
            lon_w = i
            break

    lon_e = 180
    for i in xrange(-180, 181, 30):
        if i >= lons_max:
            lon_e = i
            break

    return [lat_s, lat_n, lon_w, lon_e]


# ######################## 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    ARGS = sys.argv[1:]
    HELP_INFO = \
        u"""
        [arg1]： L2 or L3
        [arg2]： YYYYMM
        [example]： python app.py arg1 arg2
        """
    if "-h" in ARGS:
        print HELP_INFO
        sys.exit(-1)

    if len(ARGS) == 2:
        LEVEL = ARGS[0]
        YYYYMM = ARGS[1]
        with time_block("All", switch=TIME_TEST):
            main(YYYYMM, LEVEL)
    else:
        print HELP_INFO
        sys.exit(-1)

# coding:utf-8
"""
绘制 HDF5.dataset 的快视图。支持真彩图和灰度图
~~~~~~~~~~~~~~~~~~~
creation time : 2018 5 5
author : anning
~~~~~~~~~~~~~~~~~~~
"""
import os
import sys

import h5py
from configobj import ConfigObj
import numpy as np

from PB.CSC.pb_csc_console import LogServer
from PB import pb_io
from PB.pb_io import is_none
from PB.pb_time import time_block
from DV.dv_img import dv_rgb
from DV.dv_pub_3d import plt

TIME_TEST = True  # 时间测试


def main(sat_sensor, in_file):
    """
    绘制 HDF5.dataset 的快视图。支持真彩图和灰度图
    :param sat_sensor: 卫星+传感器
    :param in_file: HDF5 文件
    :return: 
    """
    # ######################## 初始化 ###########################
    # 加载程序配置文件
    proj_cfg_file = os.path.join(MAIN_PATH, "global.yaml")
    proj_cfg = pb_io.load_yaml_config(proj_cfg_file)
    if proj_cfg is None:
        LOG.error("Not find the config file: {}".format(proj_cfg_file))
        return
    else:
        # 加载配置信息
        try:
            DATASET = proj_cfg["plt_quick_view"][sat_sensor].get("rgb").get("dataset")
            LEGEND_RANGE = proj_cfg["plt_quick_view"][sat_sensor].get("img").get("colorbar_range")
            SUFFIX = proj_cfg["plt_quick_view"][sat_sensor].get("rgb").get("suffix")
            if pb_io.is_none(DATASET, SUFFIX):
                LOG.error("Yaml args is not completion. : {}".format(proj_cfg_file))
                return
        except Exception as why:
            print why
            LOG.error("Please check the yaml plt_gray args")
            return
    # ######################## 开始处理 ###########################
    print '-' * 100
    print "Start plot quick view picture."
    if not os.path.isfile(in_file):
        LOG.error("File not exist: {}".format(in_file))
        return

    in_file_name = os.path.splitext(in_file)[0]

    # 绘制真彩图
    out_picture = "{}_{}.{}".format(in_file_name, SUFFIX, "png")

    # 如果文件已经存在，跳过
    # if os.path.isfile(out_picture):
    #     print "File is already exist, skip it: {}".format(out_picture)
    #     return
    r_set, g_set, b_set = DATASET
    rgb = RGB(in_file, r_set, g_set, b_set, out_picture)
    rgb.plot()
    if not rgb.error:
        print "Output picture: {}".format(out_picture)
        print '-' * 100

    # 绘制热度图
    for legend in LEGEND_RANGE:
        dataset_name = legend[0]  # 数据集名称
        vmax = float(legend[1])  # color bar 范围 最大值
        vmin = float(legend[2])  # color bar 范围 最小值

        out_picture = "{}_{}.{}".format(in_file_name, dataset_name, "png")
        # 如果文件已经存在，跳过
        # if os.path.isfile(out_picture):
        #     print "File is already exist, skip it: {}".format(out_picture)
        #     return

        heat_map = {
            "vmin": vmin,
            "vmax": vmax,
            "cmap": "jet",
            "fill_value": -32767,
        }

        lats, lons = get_lats_lons(in_file)
        lat_lon_text = _get_lat_lon_text(lats, lons)
        if is_none(lats, lons):
            lat_lon_line = None
        else:
            lat_lon_line = {
                "lats": lats,  # 经度数据集名称
                "lons": lons,  # 维度数据集名称
                "step": 5.0,  # 线密度
                "line_width": 0.01,
                "text": lat_lon_text,
            }

        quick_view = QuickView(in_file, dataset_name, out_picture, main_view=heat_map,
                               lat_lon_line=lat_lon_line)
        quick_view.plot()
        if not quick_view.error:
            print "Output picture: {}".format(out_picture)
        else:
            print "Quick view error: {}".format(in_file)
            print '-' * 100


def get_lats_lons(hdf5_file):
    """
    获取经纬度数据
    """
    lats = None
    lons = None

    try:
        with h5py.File(hdf5_file, 'r') as h5:
            lats = h5.get("Latitude")[:]
            lons = h5.get("Longitude")[:]

    except ValueError as why:
        print why

    return lats, lons


def _get_lat_lon_text(lats, lons):
    """
    获取在图片上添加的经纬度文字
    :return:
    """
    left_top = "{:5.2f}:{:5.2f}".format(lats[0][0], lons[0][0])
    left_bottom = "{:5.2f}:{:5.2f}".format(lats[-1][0], lons[-1][0])
    right_top = "{:5.2f}:{:5.2f}".format(lats[0][-1], lons[0][-1])
    right_bottom = "{:5.2f}:{:5.2f}".format(lats[-1][-1], lons[-1][-1])

    text = {
        "left_top": left_top,
        "left_bottom": left_bottom,
        "right_top": right_top,
        "right_bottom": right_bottom,
    }

    return text


class RGB(object):
    """
    绘制真彩图
    """
    def __init__(self, hdf5_file=None, dataset_r=None, dataset_g=None, dataset_b=None,
                 out_picture=None):
        """
        初始化一个实例
        :param hdf5_file:
        :param dataset_r: r 数据dataset的名字
        :param dataset_g: g 数据dataset的名字
        :param dataset_b: b 数据dataset的名字
        :param out_picture: 输出的图片文件
        """
        self.error = False

        self.hdf5_file = hdf5_file
        self.dataset_r = dataset_r
        self.dataset_g = dataset_g
        self.dataset_b = dataset_b
        self.out_picture = out_picture

        self._get_data()

    def _get_data(self):
        """
        获取rgb的数据
        :return:
        """
        if self.error:
            return
        try:
            with h5py.File(self.hdf5_file, 'r') as h5:
                self.data_r = h5.get(self.dataset_r)[:]
                self.data_g = h5.get(self.dataset_g)[:]
                self.data_b = h5.get(self.dataset_b)[:]
        except ValueError as why:
            print why
            self.error = True
    
    def plot(self):
        """
        绘制真彩图
        :return: 
        """
        if self.error:
            return
        try:
            dv_rgb(self.data_r, self.data_g, self.data_b, self.out_picture)
        except Exception as why:
            print why
            self.error = True
            return


class QuickView(object):
    """
    绘制快视图
    """
    def __init__(self, hdf5_file=None, dataset_name=None, out_picture=None, **kwargs):
        """
        初始化一个实例
        :param hdf5_file:
        :param vmin: 热度范围最小值
        :param vmax: 热度范围最大值
        :param dataset: b 数据dataset的名字
        :param out_picture: 输出的图片文件
        """
        self.error = False

        self.hdf5_file = hdf5_file
        self.dataset_name = dataset_name
        self.out_picture = out_picture

        if kwargs is not None:
            self.main_view = kwargs.get("main_view", {})
            self.lat_lon_line = kwargs.get("lat_lon_line", {})
        else:
            self.main_view = {}
            self.lat_lon_line = {}

        self.facecolor = self.main_view.get("facecolor", "white")  # 背景色
        self.picture_width = self.main_view.get("picture_width", 5.0)  # 图片宽度
        self.cmap = self.main_view.get("cmap", "gray")  # colorbar 类型
        self.vmin = self.main_view.get("vmin", None)  # colorbar 最小值
        self.vmax = self.main_view.get("vmax", None)  # colorbar 最大值
        self.fill_value = self.main_view.get("fill_value", None)  # 填充值

        self.lats = self.lat_lon_line.get("lats", None)  # 经度数据
        self.lons = self.lat_lon_line.get("lons", None)  # 维度数据
        self.step = self.lat_lon_line.get("step", 5.0)  # 线密度
        self.line_width = self.lat_lon_line.get("line_width", 0.05)  # 线宽度
        self.text = self.lat_lon_line.get("text", None)

        self._get_data()  # 获取数据

    def _get_data(self):
        """
        获取dataset的数据，并且掩盖掉无效值
        :return:
        """
        if self.error:
            return
        try:
            with h5py.File(self.hdf5_file, 'r') as h5:
                dataset = h5.get(self.dataset_name)
                try:
                    slope = dataset.attrs["Slope"]
                    intercept = dataset.attrs["Intercept"]
                except Exception:
                    slope = 1
                    intercept = 0

                self.data = dataset[:] * slope + intercept
                self.data = np.ma.masked_where(self.data <= 0, self.data)

                if self.fill_value is not None:
                    self.data = np.ma.masked_equal(self.data, self.fill_value)

                log_set = ["Ocean_CHL1", "Ocean_CHL2", "Ocean_PIG1", "Ocean_TSM", "Ocean_YS443", ]
                if self.dataset_name in log_set:
                    idx = np.where(self.data > 0)
                    self.data[idx] = np.log10(self.data[idx])

        except ValueError as why:
            print why
            self.error = True

    def _get_lat_lon_line(self):
        """
        获取经度线和纬度线信息
        :return:
        """
        if self.error:
            return

        try:
            if self.lats is not None and self.lons is not None:
                lat_min = self.lats[0][-1]  # (0, 2047)
                lat_max = self.lats[-1][0]  # (1999, 0)

                lon_min = self.lons[0][-1]
                lon_max = self.lons[-1][0]

                lat = lat_min
                lw = self.line_width
                idx_line = None
                while lat < lat_max:
                    idx = np.logical_and(lat - lw < self.lats, self.lats < lat + lw)
                    idx = np.where(idx)
                    if idx_line is None:
                        idx_line = idx
                    else:
                        idx_line = np.concatenate((idx, idx_line), axis=1)
                    lat += self.step

                lon = lon_min
                while lon < lon_max:
                    idx = np.logical_and(lon - lw < self.lons, self.lons < lon + lw)
                    idx = np.where(idx)
                    if idx_line is None:
                        idx_line = idx
                    else:
                        idx_line = np.concatenate((idx, idx_line), axis=1)
                    lon += self.step

                self.line_lat = idx_line[0]
                self.line_lon = idx_line[1]
        except Exception as why:
            print why
            self.error = True
            return

    def _get_picture_size(self, picture_size):
        """
        根据数据集的大小获取图片大小。2000:5
        :return:
        """
        if self.error:
            return
        # 计算宽和高
        h, w = self.data.shape
        self.wight = picture_size * 1. * w / w
        self.height = picture_size * 1. * h / w

    def add_text(self, ax):
        """
        添加文字，四个角
        :return:
        """
        for k, v in self.text.items():
            shape = self.data.shape
            range_x = shape[0]
            range_y = shape[1]
            step_x = range_x / 20.0
            step_y = range_y / 20.0
            if k == "left_top":
                x = 0
                y = 0 + step_y
            elif k == "left_bottom":
                x = 0
                y = range_y - step_y
            elif k == "right_top":
                x = range_x - step_x * 2.5
                y = 0 + step_y
            else:
                x = range_x - step_x * 2.5
                y = range_y - step_y
            ax.text(x, y, v, fontsize=7)

    def plot(self):
        """
        绘制快视图
        :return:
        """
        if self.error:
            return
        try:

            # 图片大小
            self._get_picture_size(self.picture_width)
            fig = plt.figure(figsize=(self.wight, self.height))
            ax1 = plt.subplot2grid((1, 1), (0, 0))
            # 背景色
            fig.set_facecolor(self.facecolor)

            # 增加经纬度线
            if self.lat_lon_line is not None:
                self._get_lat_lon_line()
                if self.error:
                    return
                ax1.scatter(self.line_lon, self.line_lat, s=0.001, alpha=1)

            # 添加经纬度的文字
            if self.text is not None:
                self.add_text(ax1)

            # 绘主图
            plt.imshow(self.data, cmap=plt.get_cmap(self.cmap), vmin=self.vmin, vmax=self.vmax)
            ax1.axis('off')
            plt.tight_layout()
            fig.subplots_adjust(bottom=0, top=1, left=0, right=1)
            colorbar_position = fig.add_axes([0.2, 0.95, 0.6, 0.03])
            cb = plt.colorbar(cax=colorbar_position, orientation='horizontal')
            cb.ax.tick_params(labelsize=8)

            pb_io.make_sure_path_exists(os.path.dirname(self.out_picture))
            fig.savefig(self.out_picture, dpi=200)
            fig.clear()
            plt.close()

        except Exception as why:
            print why
            self.error = True
            return


# ######################## 程序全局入口 ##############################
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

    # 获取全局配置
    # GLOBAL_CFG = pb_io.get_global_cfg()
    # GLOBAL_YAML = pb_io.get_global_yaml()

    # TODO 下面读取全局配置，改为在程序配置中增加日志的输出目录
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

    # 开启进程池
    # thread_number = IN_CFG["CROND"]["threads"]
    # thread_number = 1
    # pool = Pool(processes=int(thread_number))

    if not len(ARGS) == 1:
        print HELP_INFO
    else:
        FILE_PATH = ARGS[0]
        SAT = IN_CFG["PATH"]["sat"]
        SENSOR = IN_CFG["PATH"]["sensor"]
        SAT_SENSOR = "{}+{}".format(SAT, SENSOR)

        with time_block("Plot quick view time:", switch=TIME_TEST):
            main(SAT_SENSOR, FILE_PATH)
            # pool.apply_async(main, (sat_sensor, file_path))
            # pool.close()
            # pool.join()

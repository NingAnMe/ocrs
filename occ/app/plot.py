#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/6/8 10:32
@Author  : AnNing
"""
import os
from dateutil.relativedelta import relativedelta

import h5py
import numpy as np
from DV import dv_map
from DV import dv_map_oc
from DV.dv_img import dv_rgb
from DV.dv_plot import plt, FONT0, Histogram, TimeSeries, Scatter, colors
from PB import pb_io
from PB.pb_io import make_sure_path_exists
from PB.pb_time import time_block

DEBUG = False
TIME_TEST = False

RED = '#f63240'
BLUE = '#1c56fb'
GRAY = '#c0c0c0'
EDGE_GRAY = '#303030'


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

    def __init__(self, hdf5_file=None, dataset_name=None, out_picture=None, main_view=None,
                 lat_lon_line=None):
        """
        初始化一个实例
        :param hdf5_file:
        :param main_view:
        :param lat_lon_line:
        :param dataset_name: b 数据dataset的名字
        :param out_picture: 输出的图片文件
        """
        self.error = False

        self.hdf5_file = hdf5_file
        self.dataset_name = dataset_name
        self.out_picture = out_picture

        self.main_view = main_view if main_view is not None else {}
        self.lat_lon_line = lat_lon_line if lat_lon_line is not None else {}

        self.facecolor = self.main_view.get("facecolor", "white")  # 背景色
        self.picture_width = self.main_view.get("picture_width", 5.0)  # 图片宽度
        self.cmap = self.main_view.get("cmap", "gray")  # colorbar 类型
        self.vmin = self.main_view.get("vmin", None)  # colorbar 最小值
        self.vmax = self.main_view.get("vmax", None)  # colorbar 最大值
        self.fill_value = self.main_view.get("fill_value", None)  # 填充值
        self.colorbar_ticks = None
        self.colorbar_tick_label = None

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
                if "Slope" in dataset.attrs:
                    slope = dataset.attrs["Slope"]
                else:
                    slope = 1
                if "Intercept" in dataset.attrs:
                    intercept = dataset.attrs["Intercept"]
                else:
                    intercept = 0

                self.data = dataset[:] * slope + intercept
                self.data = np.ma.masked_where(self.data <= 0, self.data)

                if self.fill_value is not None:
                    self.data = np.ma.masked_equal(self.data, self.fill_value)

                # ############对特殊数据集的值进行处理
                # 有一些数据集的数据在绘图时需要取对数，否则无法区分
                log_set = ["Ocean_CHL1", "Ocean_CHL2", "Ocean_PIG1", "Ocean_TSM", "Ocean_YS443", ]
                if self.dataset_name in log_set:
                    idx = np.where(self.data > 0)
                    self.data[idx] = np.log10(self.data[idx])
                # 有一些数据按照原来的 slope 不对，需要乘 10
                if "Rw" in self.dataset_name:
                    self.data = self.data * 10
                # #################################
        except ValueError as why:
            print "_get_data: {}".format(why)
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
                lat_min = self.lats.min()
                lat_max = self.lats.max()

                lon_min = self.lons.min()
                lon_max = self.lons.max()

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
            print "_get_lat_lon_line: {}".format(why)
            self.error = True

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

            if self.main_view is not None and "colorbar_ticks" in self.main_view:
                self.colorbar_ticks = self.main_view["colorbar_ticks"]
                cb.set_ticks(self.colorbar_ticks)
            if self.main_view is not None and "colorbar_tick_label" in self.main_view:
                self.colorbar_tick_label = self.main_view["colorbar_tick_label"]
                cb.set_ticklabels(self.colorbar_tick_label)

            pb_io.make_sure_path_exists(os.path.dirname(self.out_picture))
            fig.savefig(self.out_picture, dpi=200)
            fig.clear()
            plt.close()

        except Exception as why:
            print "plot: {}".format(why)
            self.error = True
            return


class PlotMapL3(object):
    """
    绘制 L3 产品的全球投影绘图
    """

    def __init__(self, in_file, dataset_name, out_file, map_=None):
        self.error = False
        self.in_file = in_file
        self.dataset_name = dataset_name
        self.out_file = out_file
        self.map = map_

    def draw_combine(self):
        """
        通过日合成文件，画数据集的全球分布图
        文件中需要有 Latitude 和Longitude 两个数据集
        :return:
        """
        try:
            with h5py.File(self.in_file, 'r') as h5:
                dataset = h5.get(self.dataset_name)

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
            print "Don't have enough valid value： {}  {}".format(self.dataset_name, len(idx[0]))
            return
        else:
            print "{} valid value count: {}".format(self.dataset_name, len(idx[0]))

        value = value[idx]
        lats = lats[idx]
        lons = lons[idx]

        # ############对特殊数据集的值进行处理
        # 有一些数据集的数据在绘图时需要取对数，否则无法区分
        if self.map is not None and "log10" in self.map:
            if self.map["log10"]:
                value = np.log10(value)
        # 有一些数据按照原来的 slope 不对，需要乘 10
        if "Rw" in self.dataset_name:
            value = value * 10
        # #################################

        if DEBUG:
            print "-" * 100
            print self.dataset_name
            d = np.histogram(value, bins=[x * 0.05 for x in xrange(-40, 80)])
            for i in xrange(len(d[0])):
                print "{:10} :: {:10}".format(d[1][i], d[0][i])
            print value.min()
            print value.max()
            print "-" * 100

        p = dv_map_oc.dv_map()
        p.show_bg_color = True
        p.colorbar_fmt = "%0.2f"

        if self.map is not None and "title" in self.map:
            title = self.map["title"]
        else:
            title = self.dataset_name

        # 绘制经纬度线
        if self.map is not None and "lat_lon_line" in self.map:
            lat_lon_line = self.map["lat_lon_line"]
            delat = lat_lon_line["delat"]
            delon = lat_lon_line["delon"]
            p.delat = delat  # 30
            p.delon = delon  # 30
            p.show_line_of_latlon = True
        else:
            p.show_line_of_latlon = False

        # 是否绘制某个区域
        if self.map is not None and "area_range" in self.map:
            area_range = self.map["area_range"]
            lat_s = float(area_range.get("lat_s"))
            lat_n = float(area_range.get("lat_n"))
            lon_w = float(area_range.get("lon_w"))
            lon_e = float(area_range.get("lon_e"))
            box = [lat_s, lat_n, lon_w, lon_e]
        else:
            box = None

        # 是否设置 colorbar 范围
        if self.map is not None and "legend" in self.map:
            legend = self.map["legend"]
            vmin = legend["vmin"]
            vmax = legend["vmax"]
            # 是否填写 colorbar title
            if "label" in legend:
                colorbar_label = legend["label"]
                p.colorbar_label = colorbar_label
            if "ticks" in legend:
                p.colorbar_ticks = legend["ticks"]
            if "tick_labels" in legend:
                p.colorbar_tick_labels = legend["tick_labels"]
        else:
            vmin = vmax = None

        p.title = title
        with time_block("plot combine map", switch=TIME_TEST):
            p.easyplot(lats, lons, value, ptype=None, vmin=vmin, vmax=vmax, box=box, markersize=0.1,
                       marker='o')
            # p.easyplot(lats, lons, value, ptype="pcolormesh", vmin=vmin, vmax=vmax, box=box)
            pb_io.make_sure_path_exists(os.path.dirname(self.out_file))
            p.savefig(self.out_file, dpi=300)


def plot_scatter(data_x=None, data_y=None, out_file=None, title=None,
                 x_range=None, y_range=None, x_label=None, y_label=None, annotate=None,
                 ymd_start=None, ymd_end=None, ymd=None,
                 ):
    main_path = os.path.dirname(os.path.dirname(__file__))
    style_file = os.path.join(main_path, "cfg", 'histogram.mplstyle')
    plt.style.use(style_file)
    fig = plt.figure(figsize=(6, 4))
    # fig.subplots_adjust(top=0.88, bottom=0.11, left=0.12, right=0.97)

    ax1 = plt.subplot2grid((1, 1), (0, 0))

    ax = Scatter(ax1)
    if x_range:
        ax.set_x_axis_range(x_range[0], x_range[1])
    if y_range:
        ax.set_y_axis_range(y_range[0], y_range[1])

    if x_label:
        ax.set_x_label(x_label)
    if y_label:
        ax.set_y_label(y_label)

    if annotate:
        ax.set_annotate(annotate=annotate)

    size = 1
    alpha = 0.8  # 透明度
    marker = "o"  # 形状
    color = "b"  # 颜色
    ax.set_scatter(size=size, alpha=alpha, marker=marker, color=color)

    ax.plot_scatter(data_x=data_x, data_y=data_y)

    # --------------------
    plt.tight_layout()
    fig.suptitle(title, fontsize=11, fontproperties=FONT0)
    fig.subplots_adjust(bottom=0.2, top=0.88)

    if ymd_start and ymd_end:
        fig.text(0.50, 0.02, '%s-%s' % (ymd_start, ymd_end), fontproperties=FONT0)
    elif ymd:
        fig.text(0.50, 0.02, '%s' % ymd, fontproperties=FONT0)

    fig.text(0.8, 0.02, 'OCC', fontproperties=FONT0)
    # ---------------
    make_sure_path_exists(os.path.dirname(out_file))
    fig.savefig(out_file)
    fig.clear()
    plt.close()
    print '>>> {}'.format(out_file)


def plot_regression(data_x=None, data_y=None, out_file=None, title=None,
                    x_label=None, y_label=None, annotate=None,
                    ymd_start=None, ymd_end=None, ymd=None,
                    point_color=True, plot_slope=True,
                    ):
    main_path = os.path.dirname(os.path.dirname(__file__))
    style_file = os.path.join(main_path, "cfg", 'histogram.mplstyle')
    plt.style.use(style_file)
    fig = plt.figure(figsize=(6, 6))
    # fig.subplots_adjust(top=0.88, bottom=0.11, left=0.12, right=0.97)

    ax1 = plt.subplot2grid((1, 1), (0, 0))
    # 绘制回归线
    max_value = np.nanmax(data_x)
    min_value = np.nanmin(data_x)
    color_regression = '#ff0000'
    width_regression = 1.0
    ab = np.polyfit(data_x, data_y, 1)
    p1 = np.poly1d(ab)
    p1_max = p1(max_value)
    p1_min = p1(min_value)
    ax1.plot([min_value, max_value], [p1_min, p1_max], color=color_regression,
             linewidth=width_regression, zorder=100)

    # 绘制对角线
    color_diagonal = '#888888'
    width_diagonal = 1.0
    max_value = abs(np.nanmax(np.concatenate((data_x, data_y))))
    min_value = -1 * max_value
    ax1.plot([min_value, max_value], [min_value, max_value], color=color_diagonal,
             linewidth=width_diagonal, zorder=80)

    ax = Scatter(ax1)
    x_min_value = np.min(data_x)
    x_max_value = np.max(data_x)
    y_min_value = np.min(data_y)
    y_max_value = np.max(data_y)
    ax.set_x_axis_range(x_min_value, x_max_value)
    ax.set_y_axis_range(y_min_value, y_max_value)

    if x_label:
        ax.set_x_label(x_label)
    if y_label:
        ax.set_y_label(y_label)

    if annotate:
        if plot_slope:
            annotate_new = {'left_top': ['Slope={:.4f}'.format(ab[0]),
                                         'Offset={:.4f}'.format(ab[1])]}
            annotate_new['left_top'].extend(annotate['left_top'])
        else:
            annotate_new = annotate
        ax.set_annotate(annotate=annotate_new)

    size = 1
    alpha = 0.8  # 透明度
    marker = "o"  # 形状
    color = "b"  # 颜色
    ax.set_scatter(size=size, alpha=alpha, marker=marker, color=color)

    # kde 是否绘制密度点颜色
    if point_color:
        ax.plot_scatter(data_x=data_x, data_y=data_y, kde=True)
        plt.colorbar(ax.plot_result)
    else:
        ax.plot_scatter(data_x=data_x, data_y=data_y, kde=False)
    # --------------------
    plt.tight_layout()
    fig.suptitle(title, fontsize=11, fontproperties=FONT0)
    fig.subplots_adjust(bottom=0.13, top=0.88)

    if ymd_start and ymd_end:
        fig.text(0.50, 0.02, '%s-%s' % (ymd_start, ymd_end), fontproperties=FONT0)
    elif ymd:
        fig.text(0.50, 0.02, '%s' % ymd, fontproperties=FONT0)

    fig.text(0.8, 0.02, 'OCC', fontproperties=FONT0)
    # ---------------
    make_sure_path_exists(os.path.dirname(out_file))
    fig.savefig(out_file)
    fig.clear()
    plt.close()
    print '>>> {}'.format(out_file)


def plot_bias_map(lat=None, lon=None, data=None, out_file=None,
                  title=None, vmin=None, vmax=None):
    if title:
        title = title
    else:
        title = "Map"

    # 绘制偏差的全球分布图，保持0值永远在bar的中心
    if vmin is not None and vmax is not None:
        vmin = vmin
        vmax = vmax
    else:
        datamax = np.max(data)
        if datamax >= 0:
            vmin = -1.0 * datamax
            vmax = datamax
        else:
            vmin = datamax
            vmax = -1.0 * datamax

    p = dv_map.dv_map()
    p.colorbar_fmt = '%0.3f'
    color_list = ['#000081', '#0000C8', '#1414FF', '#A3A3FF', '#FFA3A3', '#FF1414',
                  '#C70000', '#810000']
    cmap = colors.ListedColormap(color_list, 'indexed')
    p.easyplot(lat, lon, data, vmin=vmin, vmax=vmax,
               ptype=None, markersize=0.05, marker='s',
               colormap=cmap)
    p.title = title
    make_sure_path_exists(os.path.dirname(out_file))
    p.savefig(out_file)
    print '>>> {}'.format(out_file)


def plot_histogram(data=None, out_file=None,
                   title=None, x_label=None, y_label=None,
                   bins_count=200, x_range=None, hist_label=None,
                   annotate=None, ymd_start=None, ymd_end=None, ymd=None):
    """
    :param ymd_end:
    :param ymd_start:
    :param out_file: str
    :param data: np.ndarray
    :param title: str
    :param x_label: str
    :param y_label: str
    :param bins_count: int
    :param x_range: (int or float, int or float)
    :param hist_label: str
    :param annotate:
    :return:
    """
    main_path = os.path.dirname(os.path.dirname(__file__))
    style_file = os.path.join(main_path, "cfg", 'histogram.mplstyle')
    plt.style.use(style_file)
    fig = plt.figure(figsize=(6, 4))
    # fig.subplots_adjust(top=0.88, bottom=0.11, left=0.12, right=0.97)

    ax1 = plt.subplot2grid((1, 1), (0, 0))

    ax = Histogram(ax1)

    if x_range:
        ax.set_x_axis_range(x_range[0], x_range[1])

    if x_label:
        ax.set_x_label(x_label)
    if y_label:
        ax.set_y_label(y_label)

    if annotate:
        ax.set_annotate(annotate=annotate)

    ax.set_histogram(bins_count=bins_count)

    ax.set_histogram(label=hist_label)

    ax.plot_histogram(data)

    # --------------------
    plt.tight_layout()
    fig.suptitle(title, fontsize=11, fontproperties=FONT0)
    fig.subplots_adjust(bottom=0.2, top=0.88)

    if ymd_start and ymd_end:
        fig.text(0.50, 0.02, '%s-%s' % (ymd_start, ymd_end), fontproperties=FONT0)
    elif ymd:
        fig.text(0.50, 0.02, '%s' % ymd, fontproperties=FONT0)

    fig.text(0.8, 0.02, 'OCC', fontproperties=FONT0)
    # ---------------
    make_sure_path_exists(os.path.dirname(out_file))
    plt.savefig(out_file)
    fig.clear()
    plt.close()
    print '>>> {}'.format(out_file)


def plot_time_series(day_data_x=None, day_data_y=None, out_file=None, title=None,
                     x_range=None, y_range=None,
                     y_label=None, y_major_count=None, y_minor_count=None,
                     ymd_start=None, ymd_end=None, ymd=None, zero_line=True,
                     plot_background=True):
    main_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    style_file = os.path.join(main_path, "cfg", 'time_series.mplstyle')
    plt.style.use(style_file)
    fig = plt.figure(figsize=(6, 4))
    ax1 = plt.subplot2grid((1, 1), (0, 0))

    ax = TimeSeries(ax1)

    # 配置属性
    if x_range is None:
        x_range = [np.min(day_data_x), np.max(day_data_x)]
    ax.set_x_axis_range(axis_min=x_range[0], axis_max=x_range[1])
    if y_range is not None:
        ax.set_y_axis_range(axis_min=y_range[0], axis_max=y_range[1])
    if y_label is not None:
        ax.set_y_label(y_label)
    if y_major_count is not None:
        ax.set_y_major_count(y_major_count)
    if y_minor_count is not None:
        ax.set_y_minor_count(y_minor_count)

    # 绘制日数据长时间序列
    ax.set_time_series(maker='o', color=BLUE, line_width=None, marker_size=3,
                       marker_facecolor=None,
                       marker_edgecolor=BLUE, marker_edgewidth=0.3, alpha=0.8,
                       label="Daily"
                       )
    ax.plot_time_series(day_data_x, day_data_y)

    # 绘制背景填充
    if plot_background:
        month_data_x, month_data_y, month_data_std = get_month_avg_std(day_data_x, day_data_y)
        ax.set_time_series(maker='o-', color=RED, line_width=0.6, marker_size=3,
                           marker_facecolor=None,
                           marker_edgecolor=RED, marker_edgewidth=0, alpha=0.8,
                           label="Monthly")
        ax.plot_time_series(month_data_x, month_data_y)
        ax.set_background_fill(x=month_data_x,
                               y1=month_data_y - month_data_std,
                               y2=month_data_y + month_data_std,
                               color=RED,
                               alpha=0.1,
                               )
        ax.plot_background_fill()
    # 绘制 y=0 线配置，在绘制之间设置x轴范围
    if zero_line:
        ax.plot_zero_line()

    # 格式化 ax
    ax.set_ax()

    # --------------------
    plt.tight_layout()
    fig.suptitle(title, fontsize=11, fontproperties=FONT0)
    fig.subplots_adjust(bottom=0.2, top=0.88)

    if ymd_start and ymd_end:
        fig.text(0.50, 0.02, '%s-%s' % (ymd_start, ymd_end), fontproperties=FONT0)
    elif ymd:
        fig.text(0.50, 0.02, '%s' % ymd, fontproperties=FONT0)

    fig.text(0.8, 0.02, 'OCC', fontproperties=FONT0)
    # ---------------
    make_sure_path_exists(os.path.dirname(out_file))
    fig.savefig(out_file)
    fig.clear()
    plt.close()
    print '>>> {}'.format(out_file)


def get_month_avg_std(date_day, value_day):
    """
    由日数据生成月平均数据
    :param date_day: (list) [datetime 实例]
    :param value_day: (list)
    :return: (date_month, avg_month, std_month)
    """
    date_month = []
    avg_month = []
    std_month = []

    date_day = np.array(date_day)
    value_day = np.array(value_day)

    ymd_start = np.nanmin(date_day)  # 第一天日期
    ymd_end = np.nanmax(date_day)  # 最后一天日期
    month_date_start = ymd_start - relativedelta(days=(ymd_start.day - 1))  # 第一个月第一天日期

    while month_date_start <= ymd_end:
        # 当月最后一天日期
        month_date_end = month_date_start + relativedelta(months=1) - relativedelta(days=1)

        # 查找当月所有数据
        month_idx = np.logical_and(date_day >= month_date_start, date_day <= month_date_end)
        value_month = value_day[month_idx]

        avg = np.nanmean(value_month)
        std = np.nanstd(value_month)
        date_month = np.append(date_month, month_date_start + relativedelta(days=14))
        avg_month = np.append(avg_month, avg)
        std_month = np.append(std_month, std)

        month_date_start = month_date_start + relativedelta(months=1)
    return date_month, avg_month, std_month

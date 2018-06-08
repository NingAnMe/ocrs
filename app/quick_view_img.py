#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/6/8 10:32
@Author  : AnNing
"""
import os

import h5py
import numpy as np

from PB import pb_io
from DV.dv_img import dv_rgb
from DV.dv_pub_3d import plt


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

        if kwargs is None:
            kwargs = {}

        self.main_view = kwargs.get("main_view", {})
        self.lat_lon_line = kwargs.get("lat_lon_line", {})

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

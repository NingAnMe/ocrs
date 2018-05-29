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

from PB.CSC.pb_csc_console import LogServer
from PB import pb_io
from PB.pb_time import time_block
from DV.dv_img import dv_rgb

import matplotlib as mpl
mpl.use("agg")
import matplotlib.pyplot as plt


TIME_TEST = True  # 时间测试


def run(sat_sensor, hdf5_file):
    ######################### 初始化 ###########################
    # 加载程序配置文件
    proj_cfg_file = os.path.join(MAIN_PATH, "global.yaml")
    proj_cfg = pb_io.load_yaml_config(proj_cfg_file)
    if proj_cfg is None:
        LOG.error("Not find the config file: {}".format(proj_cfg_file))
        return
    else:
        # 加载配置信息
        try:
            datasets = proj_cfg["plt_quick_view"][sat_sensor].get("datasets")
            filename_suffix = proj_cfg["plt_quick_view"][sat_sensor].get("filename_suffix")
            if pb_io.is_none(datasets, filename_suffix):
                LOG.error("Yaml args is not completion. : {}".format(proj_cfg_file))
                return
        except Exception as why:
            print why
            LOG.error("Please check the yaml plt_gray args")
            return
    ######################### 开始处理 ###########################
    print '-' * 100
    print "Start plot quick view picture."
    if not os.path.isfile(hdf5_file):
        LOG.error("File not exist: {}".format(hdf5_file))
        return

    file_name = os.path.splitext(hdf5_file)[0]
    out_pic = "{}_{}.{}".format(file_name, filename_suffix, "png")

    # 如果文件已经存在，跳过
    # if os.path.isfile(out_pic):
    #     print "File is already exist, skip it: {}".format(out_pic)
    #     return

    try:
        with h5py.File(hdf5_file, 'r') as h5:
            if len(datasets) == 3:
                datas = []
                for set_name in datasets:
                    data = h5.get(set_name)[:]
                    datas.append(data)
                dv_rgb(datas[2], datas[1], datas[0], out_pic)

                print "Output picture: {}".format(out_pic)
                print '-' * 100

            for set_name in h5.keys():
                out_pic_one = out_pic.replace(filename_suffix, set_name)
                # 如果文件已经存在，跳过
                # if os.path.isfile(out_pic_one):
                #     print "File is already exist, skip it: {}".format(out_pic)
                #     return
                data = h5.get(set_name)[:]
                # 计算宽和高
                h, w = data.shape
                wight = 5. * w / w
                height = 5. * h / w
                fig = plt.figure(figsize=(wight, height))

                plt.imshow(data, cmap=plt.get_cmap("gray"))
                plt.axis('off')
                plt.tight_layout()
                fig.subplots_adjust(bottom=0, top=1, left=0, right=1)
                pb_io.make_sure_path_exists(os.path.dirname(out_pic))
                fig.savefig(out_pic_one, dpi=200)
                fig.clear()
                plt.close()

                print "Output picture: {}".format(out_pic_one)
                print '-' * 100
            else:
                LOG.error("datasets must be 1 or 3")
                return
    except Exception as why:
        print why
        LOG.error("Plot quick view picture error: {}".format(hdf5_file))
        return



######################### 程序全局入口 ##############################
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
            run(SAT_SENSOR, FILE_PATH)
            # pool.apply_async(run, (sat_sensor, file_path))
            # pool.close()
            # pool.join()

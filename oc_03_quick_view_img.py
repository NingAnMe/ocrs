# coding:utf-8
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
    proj_cfg_file = os.path.join(main_path, "global.yaml")
    proj_cfg = pb_io.load_yaml_config(proj_cfg_file)
    if proj_cfg is None:
        log.error("Not find the config file: {}".format(proj_cfg_file))
        return
    else:
        # 加载配置信息
        try:
            datasets = proj_cfg["plt_quick_view"][sat_sensor].get("datasets")
            filename_suffix = proj_cfg["plt_quick_view"][sat_sensor].get("filename_suffix")
            if pb_io.is_none(datasets, filename_suffix):
                log.error("Yaml args is not completion. : {}".format(proj_cfg_file))
                return
        except Exception as why:
            print why
            log.error("Please check the yaml plt_gray args")
            return
    ######################### 开始处理 ###########################
    print '-' * 100
    print "Start plot quick view picture."
    if not os.path.isfile(hdf5_file):
        log.error("File not exist: {}".format(hdf5_file))
        return

    file_name = os.path.splitext(hdf5_file)[0]
    out_pic = "{}_{}.{}".format(file_name, filename_suffix, "png")

    # 如果文件已经存在，跳过
    if os.path.isfile(out_pic):
        print "File is already exist, skip it: {}".format(out_pic)
        return

    try:
        with h5py.File(hdf5_file, 'r') as h5:
            if len(datasets) == 3:
                datas = []
                for set_name in datasets:
                    datas.append(h5.get(set_name)[:])
                dv_rgb(datas[0], datas[1], datas[2], out_pic)
            elif len(datasets) == 1:
                for set_name in datasets:
                    data = h5.get(set_name)[:]
                    # 计算宽和高
                    h, w = data.shape
                    wight = 5. * w / w
                    height = 5. * h / w
                    fig = plt.figure(figsize=(wight, height))

                    plt.imshow(data, cmap=plt.cm.gray)
                    plt.axis('off')
                    plt.tight_layout()
                    fig.subplots_adjust(bottom=0, top=1, left=0, right=1)
                    pb_io.make_sure_path_exists(os.path.dirname(out_pic))
                    fig.savefig(out_pic, dpi=200)
                    fig.clear()
                    plt.close()
            else:
                log.error("datasets must be 1 or 3")
                return
    except Exception as why:
        print why
        log.error("Plot quick view picture error: {}".format(hdf5_file))
        return

    print "Output picture: {}".format(out_pic)
    print '-' * 100


######################### 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    args = sys.argv[1:]
    help_info = \
        """
        [参数1]：HDF5文件
        [样例]: python 程序 HDF5文件
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

        with time_block("Plot quick view time:", switch=TIME_TEST):
            run(SAT_SENSOR, FILE_PATH)
            # pool.apply_async(run, (sat_sensor, file_path))
            # pool.close()
            # pool.join()

# coding:utf-8
"""
hdf5_compress.py
对 hdf5 文件进行压缩
~~~~~~~~~~~~~~~~~~~
creation time : 2018 1 19
author : anning
~~~~~~~~~~~~~~~~~~~
运行命令：
python hdf5_compress.py YYYYMMDD-YYYYMMDD
"""

import os
import sys
import logging

import publicmodels as pm


def main(date_range):
    # 记录程序开始运行时间
    logging.info(u'开始运行')

    # 程序运行文件目录路径
    main_dir_path, main_file = os.path.split(os.path.realpath(__file__))

    # 日期范围
    date_range = date_range

    # 配置信息
    try:
        config = pm.pm_main.get_config(main_dir_path, 'ocrs.cfg')
    except ValueError:
        logging.error(u'读取配置文件失败')
        sys.exit(-1)
    IN_PATH = config['COMPRESS']['PATH']['IN_PATH']  # 输入路径
    OUT_PATH = config['COMPRESS']['PATH']['OUT_PATH']  # 输出路径

    # 获取开始日期和结束日期
    start_date, end_date = pm.pm_time.get_date_range(date_range)

    # 获取时间范围内的目录列表
    tem_dir_list = pm.pm_file.filter_dir_by_date_range(IN_PATH, start_date, end_date)

    dir_list = []
    for dir_path in tem_dir_list:
        dirs = pm.pm_file.filter_dir_by_date_range(dir_path, start_date, end_date)
        dir_list.extend(dirs)

    # 获取时间范围内的文件列表
    file_list = []
    for dir_path in dir_list:
        files = pm.pm_file.get_file_list(dir_path)
        file_list.extend(files)

    # 对文件进行压缩处理
    for f in file_list:
        ymd, hm = pm.pm_time.get_ymd_and_hm(f)
        out_dir = os.path.join(OUT_PATH, ymd[0:4], ymd)
        pm.pm_h5py.compress_hdf5(f, out_dir)
        logging.info(u'压缩成功：%s' % f)

    logging.info(u'完成压缩：%s' % date_range)


if __name__ == '__main__':
    args = sys.argv
    main(args[1])

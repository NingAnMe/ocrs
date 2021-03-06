#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2018/6/8 16:17
@Author  : AnNing
"""
from PB.pb_time import time_block
import os
import sys


TIME_TEST = True


def main(test_id):
    """
    测试水色程序
    :return:
    """
    python = "python2"
    sat_sensor1 = "FY3B+MERSI"
    sat_sensor2 = 'FY3D+MERSI'

    file1 = "/storage-space/disk1/973NCEP_data/2018/201805/fnl_20180510_00_00_c"
    file2 = "/FY3/FY3B/MERSI/L1/1000M/2013/20130101/FY3B_MERSI_GBAL_L1_20130101_0000_1000M_MS.HDF"
    file3 = "/storage-space/disk3/OceanColor/FY3B+MERSI/Granule/2013/201301/20130101/FY3B_MERSI_ORBT_L2_ASO_MLT_NUL_20130101_0000_1000M.HDF"
    file4 = "/storage-space/disk3/OceanColor/FY3B+MERSI/Cfg/20130101.yaml"
    # file5 = "/storage-space/disk3/OceanColor/FY3B+MERSI/Daily/test/FY3B_MERSI_GBAL_L3_ASO_MLT_GLL_20130101_AOAD_5000M.HDF"
    file5 = "/storage-space/disk3/OceanColor/FY3B+MERSI/Daily/2013/201301/FY3B_MERSI_GBAL_L3_ASO_MLT_GLL_20130102_AOAD_5000M.HDF"
    file6 = "/storage-space/disk3/OceanColor/FY3B+MERSI/Daily/test/test.yaml"
    file7 = "/storage-space/disk3/OceanColor/FY3B+MERSI/Daily/test/FY3B_MERSI_GBAL_L3_ASO_MLT_GLL_20130131_AOAD_5000M.HDF"
    file9 = "/storage-space/disk3/OceanColor/FY3B+MERSI/Daily/2017/201701/FY3B_MERSI_GBAL_L3_ASO_MLT_GLL_20170102_AOAD_5000M.HDF"
    file10 = "/FY3D/MERSI/L1/1000M/2018/20180101/FY3D_MERSI_GBAL_L1_20180101_0000_1000M_MS.HDF"
    if test_id == 1:
        app = "oc_a01_ncep_to_byte.py"
        arg2 = file1
        cmd = "{} {} {} {}".format(python, app, sat_sensor1, arg2)
    elif test_id == 2:
        app = "oc_a02_calibrate.py"
        arg2 = file2
        cmd = "{} {} {} {}".format(python, app, sat_sensor1, arg2)
    elif test_id == 3:
        app = "oc_b02_quick_view_img.py"
        arg2 =file3
        cmd = "{} {} {} {}".format(python, app, sat_sensor1, arg2)
    elif test_id == 4:
        app = "oc_b03_projection.py"
        arg2 = file3
        cmd = "{} {} {} {}".format(python, app, sat_sensor1, arg2)
    elif test_id == 5:
        app = "oc_b04_combine_day.py"
        arg2 = file4
        cmd = "{} {} {} {}".format(python, app, sat_sensor1, arg2)
    elif test_id == 6:
        app = "oc_c01_combine_map.py"
        arg2 = file5
        cmd = "{} {} {} {}".format(python, app, sat_sensor1, arg2)
    elif test_id == 7:
        app = "oc_b05_combine_days.py"
        arg2 = file6
        cmd = "{} {} {} {}".format(python, app, sat_sensor1, arg2)
    elif test_id == 8:
        app = "oc_c01_combine_map.py"
        arg2 = file7
        cmd = "{} {} {} {}".format(python, app, sat_sensor1, arg2)
    elif test_id == 9:
        app = "oc_c01_combine_map.py"
        arg2 = file9
        cmd = "{} {} {} {}".format(python, app, sat_sensor1, arg2)
    elif test_id == 10:
        app = "oc_a02_calibrate.py"
        arg2 = file10
        cmd = "{} {} {} {}".format(python, app, sat_sensor2, arg2)
    else:
        return

    os.system(cmd)


######################### 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    ARGS = sys.argv[1:]
    HELP_INFO = \
        u"""
        [arg1]：test_id
        [example]： python app.py arg2
        """
    if "-h" in ARGS:
        print HELP_INFO
        sys.exit(-1)

    if len(ARGS) != 1:
        print HELP_INFO
        sys.exit(-1)
    else:
        TEST_ID = int(ARGS[0])

        with time_block("test time", switch=TIME_TEST):
            main(TEST_ID)

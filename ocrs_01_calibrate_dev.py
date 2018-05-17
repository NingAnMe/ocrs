# coding:utf-8
"""
ocrs_calibrate.py
提取 OBC 文件的 SV 数据，使用矫正系数对 MERSI L1 的产品进行定标预处理
~~~~~~~~~~~~~~~~~~~
creation time : 2018 1 24
author : anning
~~~~~~~~~~~~~~~~~~~
"""
import os
import sys
import calendar
from datetime import datetime
from multiprocessing import Pool, Lock

import numpy as np
import h5py
from matplotlib.ticker import MultipleLocator

from configobj import ConfigObj
from dateutil.relativedelta import relativedelta
from numpy.lib.polynomial import polyfit
from numpy.ma.core import std, mean
from numpy.ma.extras import corrcoef

from PB.CSC.pb_csc_console import LogServer
from PB import pb_time, pb_io
from ocrs_io import loadYamlCfg

import publicmodels as pm
from publicmodels.pm_time import time_this, time_block

from ocrs_sv_extract import sv_extract
import re


def run(pair, m1000_file):
    ######################### 初始化 ###########################

    # 加载程序配置文件
    proj_cfg_file = os.path.join(main_path, "global.yaml")
    proj_cfg = loadYamlCfg(proj_cfg_file)
    if proj_cfg is None:
        log.error("File is not exist: {}".format(proj_cfg_file))
        return

    # 加载配置信息
    try:
        PROBE_M250 = proj_cfg['calibrate'][pair]['probe_m250']
        PROBE_M1000 = proj_cfg['calibrate'][pair]['probe_m1000']
        LAUNCH_DATE = proj_cfg['lanch_date'][pair.split('+')[0]]
    except ValueError:
        log.error("Load yaml config file error, please check it. : {}".format(proj_cfg_file))
        return

    ######################### MERSI L1 定标处理 ###########################
    print '-' * 100
    print 'Start calibration'

    # 获取 M1000 文件和对应 OBC 文件
    m1000 = m1000_file
    obc = get_obc_file(m1000, L1_PATH, OBC_PATH)
    if not os.path.isfile(m1000):
        log.error("File is not exist: {}".format(m1000))
        return
    elif not os.path.isfile(obc):
        log.error("File is not exist: {}".format(obc))
        return
    else:
        print m1000
        print obc

    # 获取 ymd
    ymd = get_ymd(m1000)

    # 获取 coefficient 水色波段系统定标系数， 2013年以前和2013年以后不同
    coeffs_path = os.path.join(COEFF_PATH, '{}.txt'.format(ymd[0:4]))
    if not os.path.isfile(coeffs_path):
        log.error("File is not exist: {}".format(coeffs_path))
        return
    else:
        print coeffs_path
    coeffs = np.loadtxt(coeffs_path)

    # 对 OBC 文件进行 SV 提取
    sv_250m, sv_1000m = sv_extract(obc, PROBE_M250, PROBE_M1000)

    # 获取 dsl 数据生成时间与卫星发射时间相差的天数
    dsl = pm.pm_time.get_dsl(m1000, LAUNCH_DATE)

    # 定标计算
    if int(ymd[0:4]) <= 2013:
        # 2013 年之前
        EV_1KM_RefSB, EV_250_Aggr_1KM_RefSB = calibration_before(
            m1000, sv_1000m, sv_250m, coeffs, dsl)
    else:
        # 2013 年之后
        EV_1KM_RefSB, EV_250_Aggr_1KM_RefSB = calibration_after(
            m1000, sv_1000m, sv_250m, coeffs, dsl)

    # 输出 HDF5 文件
    _dir, _name = os.path.split(m1000)
    out_file = os.path.join(OUT_PATH, ymd[0:4], ymd, _name)
    write_hdf(m1000, obc, out_file, EV_1KM_RefSB, EV_250_Aggr_1KM_RefSB,
              sv_1000m, sv_250m, coeffs, dsl)

    print ("Success")
    print '-' * 100


# ################################### 辅助函数 ###############################
def calibration_before(m1000, sv_1000m, sv_250m, coeffs, dsl):
    """
    2013年之前
    1: dn_new = dn_ev * slope_ev + intercept_ev \
    【# 使用原文件 dn_ev 计算新的 dn_new】
    4: slope = dsl ** 2 * k2 + dsl * k1 + k0  【# k0, k1, k2 是新的】
    5: arof = ((dn_new - SV_2000) * slope) * 100  【# 四舍五入取整】
    :param m1000: L1 文件
    :param sv_1000m: OBC 中提取的 SV
    :param sv_250m:  OBC 中提取的 SV
    :param coeffs:  水色波段系统定标系数
    :param dsl:  数据生成时间与卫星发射时间相差的天数
    :return:
    """
    set_names = [u'EV_1KM_RefSB', u'EV_250_Aggr.1KM_RefSB']
    datasets = pm.pm_h5py.read_dataset_hdf5(m1000, set_names)

    attrs_1000m = pm.pm_h5py.read_attr_hdf5(
        m1000, u'EV_1KM_RefSB', [u'Slope', u'Intercept'])
    attrs_250m = pm.pm_h5py.read_attr_hdf5(
        m1000, u'EV_250_Aggr.1KM_RefSB', [u'Slope', u'Intercept'])

    # 对每个通道进行循环处理
    dataset_1km = []
    for i in xrange(0, 15):
        dn_ev = datasets[u'EV_1KM_RefSB'][i]
        slope_ev = attrs_1000m[u'Slope'][i]
        intercept_ev = attrs_1000m[u'Intercept'][i]
        sv_1000m_tem = sv_1000m[i]
        coeffs_new = coeffs[i + 4]

        arof = calculate_arof_before(intercept_ev, slope_ev, dn_ev,
                                     coeffs_new, dsl, sv_1000m_tem)
        dataset_1km.append(arof)

    dataset_250 = []
    for i in xrange(0, 4):
        dn_ev = datasets[u'EV_250_Aggr.1KM_RefSB'][i]
        slope_ev = attrs_250m[u'Slope'][i]
        intercept_ev = attrs_250m[u'Intercept'][i]
        sv_250m_tem = sv_250m[i]
        coeffs_new = coeffs[i]

        arof = calculate_arof_before(intercept_ev, slope_ev, dn_ev,
                                     coeffs_new, dsl, sv_250m_tem)
        dataset_250.append(arof)

    # 将无效值填充为 65535
    dataset_1km = np.array(dataset_1km)
    dataset_250 = np.array(dataset_250)
    dataset_1km[np.isnan(dataset_1km)] = 65535
    dataset_250[np.isnan(dataset_250)] = 65535

    # 将结果中的负值填充为 65535
    idx = np.where(dataset_1km < 0)
    dataset_1km[idx] = 65535
    idx = np.where(dataset_250 < 0)
    dataset_250[idx] = 65535

    return dataset_1km, dataset_250


def calibration_after(m1000, sv_1000m, sv_250m, coeffs, dsl):
    """
    2013年之后
    1: dn_new = dn_ev * slope_ev + intercept_ev
    【# 使用原文件 dn_ev 计算新的 dn_new】
    2: slope_old = dsl**2 * k2_old + dsl * k1_old + k0_old
    【# k0, k1, k2 是原文件 RSB_Cal_Cor_Coeff 储存的】
    3: dn_new = dn_new / slope_old + dn_sv
    4: slope_new = dsl**2 * k2_new + dsl * k1_new + k0_new
    【# k0, k1, k2 是新给的】
    5: arof = ((dn_new - SV_2000) * slope_new) * 100 【# 四舍五入取整】
    :param m1000: L1 文件
    :param sv_1000m: OBC 中提取的 SV
    :param sv_250m:  OBC 中提取的 SV
    :param coeffs:  水色波段系统定标系数
    :param dsl:  数据生成时间与卫星发射时间相差的天数
    :return:
    """
    # 从 L1 文件中获取相关的数据集
    set_names = [u'EV_1KM_RefSB', u'EV_250_Aggr.1KM_RefSB',
                 u'RSB_Cal_Cor_Coeff', u'SV_1KM_RefSB',
                 u'SV_250_Aggr1KM_RefSB']
    datasets = pm.pm_h5py.read_dataset_hdf5(m1000, set_names)
    # 从 L1 文件中获取相关数据集的属性值
    attrs_1000m = pm.pm_h5py.read_attr_hdf5(m1000, u'EV_1KM_RefSB',
                                            [u'Slope', u'Intercept'])
    attrs_250m = pm.pm_h5py.read_attr_hdf5(m1000, u'EV_250_Aggr.1KM_RefSB',
                                           [u'Slope', u'Intercept'])

    # 对每个通道进行循环处理
    dataset_1km = []
    for i in xrange(0, 15):
        dn_ev = datasets[u'EV_1KM_RefSB'][i]
        slope_ev = attrs_1000m[u'Slope'][i]
        intercept_ev = attrs_1000m[u'Intercept'][i]
        coeffs_old = datasets[u'RSB_Cal_Cor_Coeff'][i + 4]
        coeffs_new = coeffs[i + 4]
        dn_sv = datasets[u'SV_1KM_RefSB'][i]
        sv_1000m_tem = sv_1000m[i]

        arof = calculate_arof_after(intercept_ev, slope_ev, dn_ev, dn_sv,
                                    coeffs_old, coeffs_new, dsl, sv_1000m_tem)

        dataset_1km.append(arof)

    dataset_250 = []
    for i in xrange(0, 4):
        dn_ev = datasets[u'EV_250_Aggr.1KM_RefSB'][i]
        slope_ev = attrs_250m[u'Slope'][i]
        intercept_ev = attrs_250m[u'Intercept'][i]
        coeffs_old = datasets[u'RSB_Cal_Cor_Coeff'][i]
        coeffs_new = coeffs[i]
        dn_sv = datasets[u'SV_250_Aggr1KM_RefSB'][i]
        sv_250m_tem = sv_250m[i]

        arof = calculate_arof_after(intercept_ev, slope_ev, dn_ev, dn_sv,
                                    coeffs_old, coeffs_new, dsl, sv_250m_tem)

        dataset_250.append(arof)

    # 将无效值填充为 65535
    dataset_1km = np.array(dataset_1km)
    dataset_250 = np.array(dataset_250)
    dataset_1km[np.isnan(dataset_1km)] = 65535
    dataset_250[np.isnan(dataset_250)] = 65535

    # 将结果中的负值填充为 65535
    idx = np.where(dataset_1km < 0)
    dataset_1km[idx] = 65535
    idx = np.where(dataset_250 < 0)
    dataset_250[idx] = 65535

    return dataset_1km, dataset_250


def calculate_arof_before(intercept_ev, slope_ev, dn_ev, coeffs, dsl, sv_tem):
    """
    13 年以前定标计算
    :return:
    """
    k0, k1, k2 = coeffs

    # 除去有效范围外的 dn 值
    dn_ev_new = np.zeros_like(dn_ev, dtype='f')
    dn_ev_new[dn_ev_new == 0] = np.nan

    idx = np.logical_and(dn_ev > 0, dn_ev <= 10000)
    dn_ev_new[idx] = dn_ev[idx]

    # 除去 sv 数据中 0 对应的 dn 值
    dn_ev = np.zeros_like(dn_ev_new, dtype='f')
    dn_ev[dn_ev == 0] = np.nan

    idx = np.where(sv_tem != 0)
    dn_ev[idx, :] = dn_ev_new[idx, :]

    # 进行计算
    dn_new = dn_ev * slope_ev + intercept_ev
    slope = (dsl ** 2) * k2 + dsl * k1 + k0
    dn_new = dn_new - sv_tem
    arof = dn_new * slope * 100

    return arof


def calculate_arof_after(intercept_ev, slope_ev, dn_ev, dn_sv,
                         coeffs_old, coeffs_new, dsl, sv_tem):
    """
    13 年以后定标计算
    :return:
    """
    k0, k1, k2 = coeffs_new
    k0_old, k1_old, k2_old = coeffs_old

    # 除去范围外的值
    dn_ev_new = np.zeros_like(dn_ev, dtype='f')
    dn_ev_new[dn_ev_new == 0] = np.nan

    idx = np.logical_and(dn_ev > 0, dn_ev <= 10000)
    dn_ev_new[idx] = dn_ev[idx]

    # 除去 sv 数据中 0 对应的 dn 值
    dn_ev = np.zeros_like(dn_ev_new, dtype='f')
    dn_ev[dn_ev == 0] = np.nan

    idx = np.where(sv_tem != 0)
    dn_ev[idx, :] = dn_ev_new[idx, :]

    # 进行计算
    dn_new = dn_ev * slope_ev + intercept_ev
    slope_old = (dsl ** 2) * k2_old + dsl * k1_old + k0_old
    dn_new = dn_new / slope_old + dn_sv
    slope = (dsl ** 2) * k2 + dsl * k1 + k0
    dn_new = dn_new - sv_tem
    arof = dn_new * slope * 100

    return arof


def write_hdf(m1000, obc, out_file, EV_1KM_RefSB, EV_250_Aggr_1KM_RefSB,
              sv_1000m, sv_250m, coeffs, dsl):
    # 创建生成输出目录
    pb_io.make_sure_path_exists(os.path.dirname(out_file))
    # 写入数据
    with h5py.File(out_file, 'w') as out_hdf5:
        with h5py.File(m1000, 'r') as m1000:
            with h5py.File(obc, 'r') as obc:
                # 读取 L1 m1000 的数据集
                ev_1km_refsb_m1000 = m1000.get(u'EV_1KM_RefSB')
                ev_250_aggr_m1000 = m1000.get(u'EV_250_Aggr.1KM_RefSB')
                rsb_cal_cor_coeff_m1000 = m1000.get(u'RSB_Cal_Cor_Coeff')
                land_sea_mask_m1000 = m1000.get(u'LandSeaMask')
                latitude_m1000 = m1000.get(u'Latitude')
                longitude_m1000 = m1000.get(u'Longitude')
                solar_zenith_m1000 = m1000.get(u'SolarZenith')
                solar_azimuth_m1000 = m1000.get(u'SolarAzimuth')
                sensor_zenith_m1000 = m1000.get(u'SensorZenith')
                sensor_azimuth_m1000 = m1000.get(u'SensorAzimuth')

                # 读取 OBC m1000 的数据集
                sv_1km_obc = obc.get(u'SV_1km')
                sv_250m_refl_obc = obc.get(u'SV_250m_REFL')

                # 创建输出文件的数据集
                out_hdf5.create_dataset(u'EV_1KM_RefSB', dtype='u2', data=EV_1KM_RefSB,
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset(u'EV_250_Aggr.1KM_RefSB', dtype='u2', data=EV_250_Aggr_1KM_RefSB,
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset(u'SV_1km', dtype='i4', data=sv_1000m,
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset(u'SV_250m_REFL', dtype='i4', data=sv_250m,
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset(u'RSB_Cal_Cor_Coeff', dtype='f4', data=coeffs,
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset(u'LandSeaMask', dtype='i1', data=land_sea_mask_m1000,
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset(u'Latitude', dtype='f4', data=latitude_m1000,
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset(u'Longitude', dtype='f4', data=longitude_m1000,
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset(u'SolarZenith', dtype='i2', data=solar_zenith_m1000,
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset(u'SolarAzimuth', dtype='i2', data=solar_azimuth_m1000,
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset(u'SensorZenith', dtype='i2', data=sensor_zenith_m1000,
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset(u'SensorAzimuth', dtype='i2', data=sensor_azimuth_m1000,
                                        compression='gzip', compression_opts=5, shuffle=True)

                # 读取输出文件的数据集
                ev_1km_refsb_out = out_hdf5.get(u'EV_1KM_RefSB')
                ev_250_aggr_out = out_hdf5.get(u'EV_250_Aggr.1KM_RefSB')
                rsb_cal_cor_coeff_out = out_hdf5.get(u'RSB_Cal_Cor_Coeff')

                sv_1km_out = out_hdf5.get(u'SV_1km')
                sv_250m_refl_out = out_hdf5.get(u'SV_250m_REFL')

                land_sea_mask_out = out_hdf5.get(u'LandSeaMask')
                latitude_out = out_hdf5.get(u'Latitude')
                longitude_out = out_hdf5.get(u'Longitude')
                solar_zenith_out = out_hdf5.get(u'SolarZenith')
                solar_azimuth_out = out_hdf5.get(u'SolarAzimuth')
                sensor_zenith_out = out_hdf5.get(u'SensorZenith')
                sensor_azimuth_out = out_hdf5.get(u'SensorAzimuth')

                # 复制原来每个 dataset 的属性
                pm.pm_h5py.copy_attrs_h5py(ev_1km_refsb_m1000, ev_1km_refsb_out)
                pm.pm_h5py.copy_attrs_h5py(ev_250_aggr_m1000, ev_250_aggr_out)
                pm.pm_h5py.copy_attrs_h5py(sv_1km_obc, sv_1km_out)
                pm.pm_h5py.copy_attrs_h5py(sv_250m_refl_obc, sv_250m_refl_out)

                try:
                    pm.pm_h5py.copy_attrs_h5py(rsb_cal_cor_coeff_m1000,
                                               rsb_cal_cor_coeff_out)
                except AttributeError:
                    rsb_cal_cor_coeff_out.attrs['Intercept'] = [0.0]
                    rsb_cal_cor_coeff_out.attrs['Slope'] = [1.0]
                    rsb_cal_cor_coeff_out.attrs['_FillValue'] = [-9999.0]
                    rsb_cal_cor_coeff_out.attrs['band_name'] = \
                        'Calibration Model:Slope=k0+k1*DSL+k2*DSL*DSL;RefFacor'\
                        '=Slope*(EV-SV);Ref=RefFacor*d*d/100/cos(SolZ) '
                    rsb_cal_cor_coeff_out.attrs['long_name'] = \
                        'Calibration Updating Model Coefficients for 19 ' \
                        'Reflective Solar Bands (1-4, 6-20) '
                    rsb_cal_cor_coeff_out.attrs['units'] = 'NO'
                    rsb_cal_cor_coeff_out.attrs['valid_range'] = [0.0, 1.0]

                pm.pm_h5py.copy_attrs_h5py(land_sea_mask_m1000, land_sea_mask_out)
                pm.pm_h5py.copy_attrs_h5py(latitude_m1000, latitude_out)
                pm.pm_h5py.copy_attrs_h5py(longitude_m1000, longitude_out)
                pm.pm_h5py.copy_attrs_h5py(solar_zenith_m1000, solar_zenith_out)
                pm.pm_h5py.copy_attrs_h5py(solar_azimuth_m1000, solar_azimuth_out)
                pm.pm_h5py.copy_attrs_h5py(sensor_zenith_m1000, sensor_zenith_out)
                pm.pm_h5py.copy_attrs_h5py(sensor_azimuth_m1000, sensor_azimuth_out)

                # 复制文件属性
                pm.pm_h5py.copy_attrs_h5py(m1000, out_hdf5)

                # 添加文件属性
                out_hdf5.attrs['dsl'] = dsl
    print "Output file: {}".format(out_file)


def get_obc_file(m1000_file, m1000_path, obc_path):
    m1000_path = m1000_path.replace("%YYYY/%YYYY%MM%DD", '')
    obc_path = obc_path.replace("%YYYY/%YYYY%MM%DD", '')

    obc_file = m1000_file.replace(m1000_path, obc_path)
    obc_file = obc_file.replace("_1000M", "_OBCXX")

    return obc_file


def get_ymd(file_str):
    pattern = ".*_(\d{8})_.*"
    prog = re.compile(pattern)
    result = prog.match(file_str)
    if result is not None:
        groups = result.groups()
        if len(groups) != 0:
            ymd = result.groups()[0]
            return ymd
    else:
        return


######################### 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    args = sys.argv[1:]
    help_info = \
        u"""
        [参数1]：SAT+SENSOR
        [参数2]：file_path
        [样例]： python ocrs_calibrate.py FY3B+MERSI /FY3/FY3B/MERSI/L1/1000M/2017/20170101/FY3B_MERSI_GBAL_L1_20170101_0045_1000M_MS.HDF
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
        print (u"配置文件不存在 %s" % config_file)
        sys.exit(-1)

    # 载入配置文件
    inCfg = ConfigObj(config_file)
    LOG_PATH = inCfg["PATH"]["OUT"]["LOG"]
    log = LogServer(LOG_PATH)

    # 开启进程池
    # thread_number = inCfg["CROND"]["threads"]
    # thread_number = 1
    # pool = Pool(processes=int(thread_number))

    if not len(args) == 2:
        print help_info
    else:
        sat_sensor = args[0]
        file_path = args[1]
        L1_PATH = inCfg["PATH"]["IN"]["L1"]  # L1 数据文件路径
        OBC_PATH = inCfg["PATH"]["IN"]["OBC"]  # OBC 数据文件路径
        COEFF_PATH = inCfg["PATH"]["IN"]["Coeff"]  # 系数文件
        OUT_PATH = inCfg["PATH"]["MID"]["Calibration"]  # 预处理文件输出路径
        with time_block("calibrate time:"):
            run(sat_sensor, file_path)
        # pool.apply_async(run, (sat_sensor, file_path))
        # pool.close()
        # pool.join()

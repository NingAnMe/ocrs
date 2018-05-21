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

import numpy as np
import h5py
from configobj import ConfigObj

from PB.CSC.pb_csc_console import LogServer
from PB import pb_io, pb_time, pb_calculate
from PB.pb_time import time_block


TIME_TEST = True  # 时间测试


def run(sat_sensor, m1000_file):
    ######################### 初始化 ###########################
    # 加载程序配置文件
    proj_cfg_file = os.path.join(main_path, "global.yaml")
    proj_cfg = pb_io.load_yaml_config(proj_cfg_file)
    if proj_cfg is None:
        log.error("File is not exist: {}".format(proj_cfg_file))
        return
    else:
        # 加载配置信息
        try:
            PROBE_M250 = proj_cfg['calibrate'][sat_sensor]['probe_m250']
            PROBE_M1000 = proj_cfg['calibrate'][sat_sensor]['probe_m1000']
            LAUNCH_DATE = proj_cfg['lanch_date'][sat_sensor.split('+')[0]]
            if pb_io.is_none(PROBE_M250, PROBE_M1000, LAUNCH_DATE):
                log.error("Yaml args is not completion. : {}".format(proj_cfg_file))
                return
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
    ymd = pb_time.get_ymd(m1000)

    # 获取输出文件
    out_path = pb_io.path_replace_ymd(OUT_PATH, ymd)
    _, _name = os.path.split(m1000)
    out_file = os.path.join(out_path, _name)

    # 如果文件已经存在，跳过
    if os.path.isfile(out_file):
        print "File is already exist, skip it: {}".format(out_file)
        return

    # 获取 coefficient 水色波段系统定标系数， 2013年以前和2013年以后不同
    coeffs_path = os.path.join(COEFF_PATH, '{}.txt'.format(ymd[0:4]))
    if not os.path.isfile(coeffs_path):
        log.error("File is not exist: {}".format(coeffs_path))
        return
    else:
        print coeffs_path
    coeffs = np.loadtxt(coeffs_path)

    # 对 OBC 文件进行 SV 提取
    SV_250m_REFL, SV_1km = sv_extract(obc, PROBE_M250, PROBE_M1000)

    # 获取 dsl 数据生成时间与卫星发射时间相差的天数
    dsl = pb_time.get_dsl(ymd, LAUNCH_DATE)

    # 定标计算
    if int(ymd[0:4]) <= 2013:
        # 2013 年之前
        EV_1KM_RefSB, EV_250_Aggr_1KM_RefSB = calibration_before(
            m1000, SV_1km, SV_250m_REFL, coeffs, dsl)
    else:
        # 2013 年之后
        EV_1KM_RefSB, EV_250_Aggr_1KM_RefSB = calibration_after(
            m1000, SV_1km, SV_250m_REFL, coeffs, dsl)

    # 输出 HDF5 文件
    write_hdf(m1000, obc, out_file, EV_1KM_RefSB, EV_250_Aggr_1KM_RefSB,
              SV_1km, SV_250m_REFL, coeffs, dsl)

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
    :param sv_1000m: OBC 中提取的 SV_1km
    :param sv_250m:  OBC 中提取的 SV_250m_REFL
    :param coeffs:  水色波段系统定标系数
    :param dsl:  数据生成时间与卫星发射时间相差的天数
    :return:
    """
    set_names = ['EV_1KM_RefSB', 'EV_250_Aggr.1KM_RefSB']
    datasets = pb_io.read_dataset_hdf5(m1000, set_names)

    attrs_1000m = pb_io.read_attr_hdf5(
        m1000, 'EV_1KM_RefSB', ['Slope', 'Intercept'])
    attrs_250m = pb_io.read_attr_hdf5(
        m1000, 'EV_250_Aggr.1KM_RefSB', ['Slope', 'Intercept'])

    # 对每个通道进行循环处理
    dataset_1km = []
    for i in xrange(0, 15):
        dn_ev = datasets['EV_1KM_RefSB'][i]
        slope_ev = attrs_1000m['Slope'][i]
        intercept_ev = attrs_1000m['Intercept'][i]
        sv_1000m_tem = sv_1000m[i]
        coeffs_new = coeffs[i + 4]

        arof = calculate_arof_before(intercept_ev, slope_ev, dn_ev,
                                     coeffs_new, dsl, sv_1000m_tem)
        dataset_1km.append(arof)

    dataset_250 = []
    for i in xrange(0, 4):
        dn_ev = datasets['EV_250_Aggr.1KM_RefSB'][i]
        slope_ev = attrs_250m['Slope'][i]
        intercept_ev = attrs_250m['Intercept'][i]
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
    :param sv_1000m: OBC 中提取的 SV_1km
    :param sv_250m:  OBC 中提取的 SV_250m_REFL
    :param coeffs:  水色波段系统定标系数
    :param dsl:  数据生成时间与卫星发射时间相差的天数
    :return:
    """
    # 从 L1 文件中获取相关的数据集
    set_names = ['EV_1KM_RefSB', 'EV_250_Aggr.1KM_RefSB',
                 'RSB_Cal_Cor_Coeff', 'SV_1KM_RefSB',
                 'SV_250_Aggr1KM_RefSB']
    datasets = pb_io.read_dataset_hdf5(m1000, set_names)
    # 从 L1 文件中获取相关数据集的属性值
    attrs_1000m = pb_io.read_attr_hdf5(m1000, 'EV_1KM_RefSB', ['Slope', 'Intercept'])
    attrs_250m = pb_io.read_attr_hdf5(m1000, 'EV_250_Aggr.1KM_RefSB', ['Slope', 'Intercept'])

    # 对每个通道进行循环处理
    dataset_1km = []
    for i in xrange(0, 15):
        dn_ev = datasets['EV_1KM_RefSB'][i]
        slope_ev = attrs_1000m['Slope'][i]
        intercept_ev = attrs_1000m['Intercept'][i]
        coeffs_old = datasets['RSB_Cal_Cor_Coeff'][i + 4]
        coeffs_new = coeffs[i + 4]
        dn_sv = datasets['SV_1KM_RefSB'][i]
        sv_1000m_tem = sv_1000m[i]

        arof = calculate_arof_after(intercept_ev, slope_ev, dn_ev, dn_sv,
                                    coeffs_old, coeffs_new, dsl, sv_1000m_tem)

        dataset_1km.append(arof)

    dataset_250 = []
    for i in xrange(0, 4):
        dn_ev = datasets['EV_250_Aggr.1KM_RefSB'][i]
        slope_ev = attrs_250m['Slope'][i]
        intercept_ev = attrs_250m['Intercept'][i]
        coeffs_old = datasets['RSB_Cal_Cor_Coeff'][i]
        coeffs_new = coeffs[i]
        dn_sv = datasets['SV_250_Aggr1KM_RefSB'][i]
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


def calculate_arof_after(intercept_ev, slope_ev, dn_ev, dn_sv, coeffs_old, coeffs_new, dsl, sv_tem):
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


def sv_extract(obc, probe_250m, probe_1000m):
    """
    提取 OBC 文件的 SV 数据
    # SV 提取(OBC文件)
    1: 250m 的前 4 个通道，先把 SV 8000*24 使用 40 个探元转换为 200 * 24 (选择 40 个探元中某个探元号的数据)，每个通道，可配置参数取某一个探元号
    2: 1000m 的 15 个通道，先把 SV 2000*6 使用 10 个探元 变为 200 * 6 (选择 10 个探元中某个探元号的数据)，每个通道，可配置参数取某一个探元号
    3: 然后按照 10 行滑动 从 200 * 6 变成 200 * 1
    4: 然后 10 行用 1 个 SV 均值，从 200 * 1 变成 2000 * 1
    5: 总共 19 * 2000 (sv_2000)
    :param obc: OBC 文件
    :param probe_250m: (list)250m 每个通道选取的探元 id
    :param probe_1000m: (list)1000m 每个通道选取的探元 id
    :return:
    """
    # 获取数据集
    setnames_obc = ['SV_1km', 'SV_250m_REFL']
    datasets_obc = pb_io.read_dataset_hdf5(obc, setnames_obc)
    # 提取 SV_250m_REFL
    dataset_250m = []
    for i in xrange(0, 4):
        dataset = datasets_obc['SV_250m_REFL'][i]
        probe_count = 40
        probe_id = probe_250m[i]

        dataset_new = sv_dataset_extract(dataset, probe_count, probe_id)
        dataset_250m.append(dataset_new)
    # 提取 SV_1km
    dataset_1000m = []
    for i in xrange(0, 15):
        dataset = datasets_obc['SV_1km'][i]
        probe_count = 10
        probe_id = probe_1000m[i]

        dataset_new = sv_dataset_extract(dataset, probe_count, probe_id)
        dataset_1000m.append(dataset_new)

    # 将数据转换为 ndarray 类
    sv_250m = np.array(dataset_250m)
    sv_1000m = np.array(dataset_1000m)

    return sv_250m, sv_1000m


def sv_dataset_extract(dataset, probe_count, probe_id):
    """
    提取某个通道 SV 数据集的数据
    :param dataset: SV 数据集
    :param probe_count: (int) 探元数量
    :param probe_id: (int) 此通道对应的探元 id
    :return:
    """
    # 筛选探元号对应的行
    dataset_ext = pb_calculate.extract_lines(dataset, probe_count, probe_id)
    # 计算 avg 和 std
    avg_std_list = pb_calculate.rolling_calculate_avg_std(dataset_ext, 10)
    # 过滤有效值
    dataset_valid = pb_calculate.filter_valid_value(dataset_ext, avg_std_list, 2)
    # 计算均值
    dataset_avg = pb_calculate.calculate_avg(dataset_valid)
    dataset_avg = np.array(dataset_avg).reshape(len(dataset_avg), 1)
    # 将行数扩大 10 倍
    dataset_avg = pb_calculate.expand_dataset_line(dataset_avg, 10)
    # 对浮点数据数据进行四舍五入
    dataset_new = np.rint(dataset_avg)
    return dataset_new


def write_hdf(m1000, obc, out_file, EV_1KM_RefSB, EV_250_Aggr_1KM_RefSB,
              SV_1km, SV_250m_REFL, RSB_Cal_Cor_Coeff, dsl):
    # 创建生成输出目录
    pb_io.make_sure_path_exists(os.path.dirname(out_file))
    # 写入数据
    with h5py.File(out_file, 'w') as out_hdf5:
        with h5py.File(m1000, 'r') as m1000:
            with h5py.File(obc, 'r') as obc:
                # M1000 文件的数据集
                dataset_m1000 = ['EV_1KM_RefSB', 'EV_250_Aggr.1KM_RefSB', 'RSB_Cal_Cor_Coeff',
                                 'LandSeaMask', 'Latitude', 'Longitude', 'SolarZenith',
                                 'SolarAzimuth', 'SensorZenith', 'SensorAzimuth']
                # OBC 文件的数据集
                dataset_obc = ['SV_1km', 'SV_250m_REFL']

                # 创建输出文件的数据集
                out_hdf5.create_dataset('EV_1KM_RefSB', dtype='u2', data=EV_1KM_RefSB,
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset('EV_250_Aggr.1KM_RefSB', dtype='u2', data=EV_250_Aggr_1KM_RefSB,
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset('SV_1km', dtype='i4', data=SV_1km,
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset('SV_250m_REFL', dtype='i4', data=SV_250m_REFL,
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset('RSB_Cal_Cor_Coeff', dtype='f4', data=RSB_Cal_Cor_Coeff,
                                        compression='gzip', compression_opts=5, shuffle=True)

                out_hdf5.create_dataset('LandSeaMask', dtype='i1', data=m1000.get('LandSeaMask')[:],
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset('Latitude', dtype='f4', data=m1000.get('Latitude')[:],
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset('Longitude', dtype='f4', data=m1000.get('Longitude')[:],
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset('SolarZenith', dtype='i2', data=m1000.get('SolarZenith')[:],
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset('SolarAzimuth', dtype='i2', data=m1000.get('SolarAzimuth')[:],
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset('SensorZenith', dtype='i2', data=m1000.get('SensorZenith')[:],
                                        compression='gzip', compression_opts=5, shuffle=True)
                out_hdf5.create_dataset('SensorAzimuth', dtype='i2', data=m1000.get('SensorAzimuth')[:],
                                        compression='gzip', compression_opts=5, shuffle=True)

                coeff_attrs = {
                    'Intercept': [0.0], 'Slope': [1.0], '_FillValue': [-9999.0],
                    'band_name': "Calibration Model:Slope=k0+k1*DSL+k2*DSL*DSL;RefFacor=Slope*(EV-SV);Ref=RefFacor*d*d/100/cos(SolZ)",
                    'long_name': "Calibration Updating Model Coefficients for 19 Reflective Solar Bands (1-4, 6-20)",
                    'units': 'NO', 'valid_range': [0.0, 1.0],
                }
                # 复制原来每个 dataset 的属性
                for dataset_name in dataset_m1000:
                    if dataset_name == "RSB_Cal_Cor_Coeff":
                        continue
                    else:
                        for k, v in m1000.get(dataset_name).attrs.items():
                            out_hdf5.get(dataset_name).attrs[k] = v
                for dataset_name in dataset_obc:
                    if dataset_name == "RSB_Cal_Cor_Coeff":
                        continue
                    else:
                        for k, v in obc.get(dataset_name).attrs.items():
                            out_hdf5.get(dataset_name).attrs[k] = v
                for k, v in coeff_attrs.items():
                    out_hdf5.get("RSB_Cal_Cor_Coeff").attrs[k] = v

                # 复制文件属性
                pb_io.copy_attrs_h5py(m1000, out_hdf5)

                # 添加文件属性
                out_hdf5.attrs['dsl'] = dsl
    print "Output file: {}".format(out_file)


def get_obc_file(m1000_file, m1000_path, obc_path):
    """
    通过 1KM 文件路径生成 OBC 文件的路径
    :param m1000_file:
    :param m1000_path:
    :param obc_path:
    :return:
    """
    m1000_path = m1000_path.replace("%YYYY/%YYYY%MM%DD", '')
    obc_path = obc_path.replace("%YYYY/%YYYY%MM%DD", '')

    obc_file = m1000_file.replace(m1000_path, obc_path)
    obc_file = obc_file.replace("_1000M", "_OBCXX")

    return obc_file


######################### 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    args = sys.argv[1:]
    help_info = \
        """
        [参数1]：L1文件
        [样例]： python 程序 L1文件
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

        L1_PATH = inCfg["PATH"]["IN"]["l1"]  # L1 数据文件路径
        OBC_PATH = inCfg["PATH"]["IN"]["obc"]  # OBC 数据文件路径
        COEFF_PATH = inCfg["PATH"]["IN"]["coeff"]  # 系数文件
        OUT_PATH = inCfg["PATH"]["MID"]["calibrate"]  # 预处理文件输出路径
        SAT = inCfg["PATH"]["sat"]
        SENSOR = inCfg["PATH"]["sensor"]
        SAT_SENSOR = "{}+{}".format(SAT, SENSOR)

        with time_block("Calibrate time:", switch=TIME_TEST):
            run(SAT_SENSOR, FILE_PATH)

        # pool.apply_async(run, (sat_sensor, file_path))
        # pool.close()
        # pool.join()

# coding:utf-8
"""
使用矫正系数对 MERSI L1 的产品进行定标预处理
~~~~~~~~~~~~~~~~~~~
creation time : 2018 1 24
author : anning
~~~~~~~~~~~~~~~~~~~
"""
import os
import sys

from configobj import ConfigObj
import h5py
import numpy as np

from PB.CSC.pb_csc_console import LogServer
from PB import pb_io, pb_time, pb_calculate
from PB.pb_time import time_block

TIME_TEST = True  # 时间测试


def main(sat_sensor, in_file):
    ######################### 初始化 ###########################
    # 加载程序配置文件
    proj_cfg_file = os.path.join(MAIN_PATH, "global.yaml")
    proj_cfg = pb_io.load_yaml_config(proj_cfg_file)
    if proj_cfg is None:
        LOG.error("File is not exist: {}".format(proj_cfg_file))
        return
    else:
        # 加载配置信息
        try:
            PROBE = proj_cfg['calibrate'][sat_sensor]['probe']
            LAUNCH_DATE = proj_cfg['lanch_date'][sat_sensor.split('+')[0]]
            PLOT = proj_cfg['calibrate'][sat_sensor]['plot']
            if pb_io.is_none(PROBE, LAUNCH_DATE):
                LOG.error("Yaml args is not completion. : {}".format(proj_cfg_file))
                return
        except ValueError:
            LOG.error("Load yaml config file error, please check it. : {}".format(proj_cfg_file))
            return

    ######################### MERSI L1 定标处理 ###########################
    print '-' * 100
    print 'Start calibration'

    # 获取 M1000 文件和对应 OBC 文件
    l1_1000m = in_file
    obc_1000m = get_obc_file(l1_1000m, L1_PATH, OBC_PATH)
    if not os.path.isfile(l1_1000m):
        LOG.error("File is not exist: {}".format(l1_1000m))
        return
    elif not os.path.isfile(obc_1000m):
        LOG.error("File is not exist: {}".format(obc_1000m))
        return
    else:
        print l1_1000m
        print obc_1000m

    ymd = pb_time.get_ymd(l1_1000m)

    # 获取 coefficient 水色波段系统定标系数， 2013年以前和2013年以后不同
    coeff_file = os.path.join(COEFF_PATH, '{}.txt'.format(ymd[0:4]))
    if not os.path.isfile(coeff_file):
        LOG.error("File is not exist: {}".format(coeff_file))
        return
    else:
        print coeff_file

    # 获取输出文件
    out_path = pb_io.path_replace_ymd(OUT_PATH, ymd)
    _name = os.path.basename(l1_1000m)
    out_file = os.path.join(out_path, _name)

    # 如果输出文件已经存在，跳过预处理
    if os.path.isfile(out_file):
        print "File is already exist, skip it: {}".format(out_file)
        return

    # 初始化一个预处理实例
    calibrate = Calibrate(l1_1000m=l1_1000m, obc_1000m=obc_1000m, coeff_file=coeff_file, out_file=out_file,
                          launch_date=LAUNCH_DATE)

    # 对 OBC 文件进行 SV 提取
    calibrate.sv_extract(PROBE)

    # 重新定标 L1 数据
    calibrate.calibrate()

    # 将新数据写入 HDF5 文件
    calibrate.write()

    # 绘图
    # if PLOT == "on":
    #     calibrate.plot()

    print("Success")
    print '-' * 100

class Calibrate(object):
    """
    使用矫正系数对 MERSI L1 的产品进行定标预处理
    """

    def __init__(self, l1_1000m=None, obc_1000m=None, coeff_file=None, out_file=None,
                launch_date=None, **kwargs):
        """
        :param l1_1000m: L1 1000m 文件
        :param obc_1000m: OBC 1000m 文件
        :param coeffs_file: 矫正系数文件，txt 格式，三列，分别为 k0 k1 k2
        :param out_path: 输出文件
        :param launch_date: 发星时间
        :return:
        """
        self.error = False
        if pb_io.is_none(l1_1000m, obc_1000m, coeff_file, launch_date):
            print "calibrate init args is error."
            self.error = True
            return
        self.l1_1000m = l1_1000m
        self.obc_1000m = obc_1000m
        self.coeff_file = coeff_file
        self.out_file = out_file
        self.launch_date = launch_date

        self._get_ymd()
        self._get_coeff()
        self._get_dsl()


    def _get_ymd(self):
        if self.error:
            return
        try:
            self.ymd = pb_time.get_ymd(self.l1_1000m)
        except Exception as why:
            print why
            self.error = True

    def _get_coeff(self):
        if self.error:
            return
        if not os.path.isfile(self.coeff_file):
            LOG.error("File is not exist: {}".format(self.coeff_file))
            self.error = True
            return
        try:
            self.coeff = np.loadtxt(self.coeff_file)
        except Exception as why:
            print why
            self.error = True

    def _get_dsl(self):
        if self.error:
            return
        try:
            self.dsl = pb_time.get_dsl(self.ymd, self.launch_date)
        except Exception as why:
            print why
            self.error = True

    def sv_extract(self, probe):
        """
        精提取 OBC 文件的 SV 值
        包括 250m reflective bands 和 1000m reflective bands

        # SV 提取(OBC文件)
        1: 250m 的前 4 个通道，先把 SV 8000*24 使用 40 个探元转换为 200 * 24 (选择 40 个探元中某个探元号的数据)，每个通道，可配置参数取某一个探元号
        2: 1000m 的 15 个通道，先把 SV 2000*6 使用 10 个探元 变为 200 * 6 (选择 10 个探元中某个探元号的数据)，每个通道，可配置参数取某一个探元号
        3: 然后按照 10 行滑动 从 200 * 6 变成 200 * 1
        4: 然后 10 行用 1 个 SV 均值，从 200 * 1 变成 2000 * 1
        5: 总共 19 * 2000 * 1(sv_2000)
        :param obc: OBC 文件
        :param probe_250m: (list)250m 每个通道选取的探元 id
        :param probe_1000m: (list)1000m 每个通道选取的探元 id
        :return:
        """
        if self.error:
            return
        if not isinstance(probe, list) or len(probe) != 19:
            print "probe arg in yaml file has error."
            self.error = True
            return
        # 获取数据集
        setnames_obc = ['SV_1km', 'SV_250m_REFL']
        datasets_obc = pb_io.read_dataset_hdf5(self.obc_1000m, setnames_obc)

        self.sv_extract_obc = []
        # 提取 SV_250m_REFL
        for i in xrange(4):
            dataset = datasets_obc['SV_250m_REFL'][i]
            probe_count = 40  # 探元总数
            probe_id = probe[i]  # 探元 id
            slide_step = 10  # 滑动的步长

            sv_dataset = self.dataset_extract(dataset, probe_count, probe_id, slide_step)
            self.sv_extract_obc.append(sv_dataset)
        # 提取 SV_1km
        for i in xrange(15):
            dataset = datasets_obc['SV_1km'][i]
            probe_count = 10  # 探元总数
            probe_id = probe[i + 4]  # 探元 id
            slide_step = 10  # 滑动的步长

            sv_dataset = self.dataset_extract(dataset, probe_count, probe_id, slide_step)
            self.sv_extract_obc.append(sv_dataset)


    def dataset_extract(self, dataset, probe_count, probe_id, slide_step=10):
        """
        提取数据集的数据, 将 x * x 的数据提取为 y * 1
        :param dataset: 二维数据集
        :param probe_count: (int) 探元总数量
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

    def calibrate(self):
        """
        进行预处理
        2013年之前
        1: ev_dn_l1 = ev_dn_l1 * slope_ev + intercept_ev 【原 L1 文件的 DN 值】
        4: slope = dsl ** 2 * k2 + dsl * k1 + k0  【# k0, k1, k2 是新的】
        5: ref_new = ((ev_dn_l1 - sv_dn_obc) * slope) * 100  【# 四舍五入取整】

        2013年之后
        1: ev_ref_l1 = ev_ref_l1 * slope_ev + intercept_ev
        2: slope_old = dsl**2 * k2_old + dsl * k1_old + k0_old
        【# k0, k1, k2 是原文件 RSB_Cal_Cor_Coeff 储存的】
        3: dn_new = ev_ref_l1 / slope_old + sv_dn_l1
        4: slope_new = dsl**2 * k2_new + dsl * k1_new + k0_new
        【# k0, k1, k2 是新给的】
        5: ref_new = ((dn_new - sv_dn_obc) * slope_new) * 100 【# 四舍五入取整】
        """
        # 定标计算
        # 发星-2013 年
        if int(self.ymd[0:4]) <= 2013:
            self.ev_250m_ref = []
            self.ev_1000m_ref = []
            for i in xrange(19):
                if i < 4:
                    ev_name = "EV_250_Aggr.1KM_RefSB"
                    k = i
                else:
                    ev_name = "EV_1KM_RefSB"
                    k = i - 4

                with h5py.File(self.l1_1000m, "r") as h5:
                    ev_dn_l1 = h5.get(ev_name)[:][k]
                    ev_slope = h5.get(ev_name).attrs["Slope"][k]
                    ev_intercept = h5.get(ev_name).attrs["Intercept"][k]

                sv_dn_obc = self.sv_extract_obc[i]

                k0, k1, k2 = self.coeff[i]

                # 除去 sv 数据中 0 对应的 dn 值
                idx = np.where(sv_dn_obc == 0)
                ev_dn_l1[idx, :] = 0

                # 除去有效范围外的 dn 值
                ev_dn_l1 = np.ma.masked_less_equal(ev_dn_l1, 0)
                ev_dn_l1 = np.ma.masked_greater(ev_dn_l1, 4095)

                # 进行计算
                ev_dn_l1 = ev_dn_l1 * ev_slope + ev_intercept
                slope = (self.dsl**2) * k2 + self.dsl * k1 + k0
                dn_new = ev_dn_l1 - sv_dn_obc
                ref_new = dn_new * slope * 100

                # 除去有效范围外的 dn 值
                ref_new = np.ma.masked_less_equal(ref_new, 0)
                ref_new.filled(0)
                ref_new = ref_new.astype(np.uint16)

                if i < 4:
                    self.ev_250m_ref.append(ref_new)
                else:
                    self.ev_1000m_ref.append(ref_new)

        # 2014 年 - 今
        else:
            self.ev_250m_ref = []
            self.ev_1000m_ref = []
            for i in xrange(19):
                if i < 4:
                    k = i
                    ev_name = "EV_250_Aggr.1KM_RefSB"
                    sv_name = "SV_250_Aggr1KM_RefSB"
                else:
                    k = i - 4
                    ev_name = "EV_1KM_RefSB"
                    sv_name = "SV_1KM_RefSB"

                with h5py.File(self.l1_1000m, "r") as h5:
                    ev_ref_l1 = h5.get(ev_name)[:][k]
                    ev_slope = h5.get(ev_name).attrs["Slope"][k]
                    ev_intercept = h5.get(ev_name).attrs["Intercept"][k]
                    sv_dn_l1 = h5.get(sv_name)[:][k]
                    coeff_old = h5.get('RSB_Cal_Cor_Coeff')[:]

                sv_dn_obc = self.sv_extract_obc[i]

                k0_new, k1_new, k2_new = self.coeff[i]
                k0_old, k1_old, k2_old = coeff_old[i]

                # 除去 sv 数据中 0 对应的 dn 值
                idx = np.where(sv_dn_obc == 0)
                ev_ref_l1[idx, :] = 0

                # 除去有效范围外的 dn 值
                ev_ref_l1 = np.ma.masked_less_equal(ev_ref_l1, 0)
                ev_ref_l1 = np.ma.masked_greater(ev_ref_l1, 10000)

                # 进行计算
                ev_ref_l1 = ev_ref_l1 * ev_slope + ev_intercept
                slope_old = (self.dsl**2) * k2_old + self.dsl * k1_old + k0_old
                dn_new = ev_ref_l1 / slope_old + sv_dn_l1
                slope_new = (self.dsl**2) * k2_new + self.dsl * k1_new + k0_new
                dn_new = dn_new - sv_dn_obc
                ref_new = dn_new * slope_new * 100

                # 除去有效范围外的 dn 值
                ref_new = np.ma.masked_less_equal(ref_new, 0)
                ref_new.filled(0)
                ref_new = ref_new.astype(np.uint16)

                if i < 4:
                    self.ev_250m_ref.append(ref_new)
                else:
                    self.ev_1000m_ref.append(ref_new)
    
    def write(self):
        """
        将处理后的数据写入 HDF5 文件
        """
        # 创建生成输出目录
        pb_io.make_sure_path_exists(os.path.dirname(self.out_file))
        # 写入数据
        with h5py.File(self.out_file, 'w') as out_hdf5:
            with h5py.File(self.l1_1000m, 'r') as m1000:
                with h5py.File(self.obc_1000m, 'r') as obc:
                    # M1000 文件的数据集
                    dataset_m1000 = [
                        'EV_1KM_RefSB', 'EV_250_Aggr.1KM_RefSB', 'RSB_Cal_Cor_Coeff', 'LandSeaMask',
                        'Latitude', 'Longitude', 'SolarZenith', 'SolarAzimuth', 'SensorZenith',
                        'SensorAzimuth'
                    ]
                    # OBC 文件的数据集
                    dataset_obc = ['SV_1km', 'SV_250m_REFL']

                    # 创建输出文件的数据集
                    out_hdf5.create_dataset('EV_250_Aggr.1KM_RefSB', dtype='u2', data=self.ev_250m_ref,
                                            compression='gzip', compression_opts=5, shuffle=True)
                    out_hdf5.create_dataset('EV_1KM_RefSB', dtype='u2', data=self.ev_1000m_ref,
                                            compression='gzip', compression_opts=5, shuffle=True)
                    out_hdf5.create_dataset('SV_1km', dtype='u2', data=self.sv_extract_obc[0:4],
                                            compression='gzip', compression_opts=5, shuffle=True)
                    out_hdf5.create_dataset('SV_250m_REFL', dtype='u2', data=self.sv_extract_obc[4:19],
                                            compression='gzip', compression_opts=5, shuffle=True)
                    out_hdf5.create_dataset('RSB_Cal_Cor_Coeff', dtype='f4', data=self.coeff,
                                            compression='gzip', compression_opts=5, shuffle=True)
                    
                    out_hdf5.create_dataset('LandSeaMask', dtype='u1', data=m1000.get('LandSeaMask')[:],
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
                        'Intercept': [0.0],
                        'Slope': [1.0],
                        '_FillValue': [-9999.0],
                        'band_name':
                        "Calibration Model:Slope=k0+k1*DSL+k2*DSL*DSL;RefFacor=Slope*(EV-SV);Ref=RefFacor*d*d/100/cos(SolZ)",
                        'long_name':
                        "Calibration Updating Model Coefficients for 19 Reflective Solar Bands (1-4, 6-20)",
                        'units':
                        'NO',
                        'valid_range': [0.0, 1.0],
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
                    out_hdf5.attrs['dsl'] = self.dsl
        print "Output file: {}".format(self.out_file)


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

        L1_PATH = IN_CFG["PATH"]["IN"]["l1"]  # L1 数据文件路径
        OBC_PATH = IN_CFG["PATH"]["IN"]["obc"]  # OBC 数据文件路径
        COEFF_PATH = IN_CFG["PATH"]["IN"]["coeff"]  # 系数文件
        OUT_PATH = IN_CFG["PATH"]["MID"]["calibrate"]  # 预处理文件输出路径
        SAT = IN_CFG["PATH"]["sat"]
        SENSOR = IN_CFG["PATH"]["sensor"]
        SAT_SENSOR = "{}+{}".format(SAT, SENSOR)

        with time_block("Calibrate time:", switch=TIME_TEST):
            main(SAT_SENSOR, FILE_PATH)

        # pool.apply_async(run, (sat_sensor, file_path))
        # pool.close()
        # pool.join()

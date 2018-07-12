# coding: utf-8
# 引用自编库
'''
Description:  静止和极轨通用匹配程序
Author:       wangpeng
Date:         2017-08-10
version:      1.0.1_beat
Input:        yaml格式配置文件
Output:       hdf5格式匹配文件  (^_^)
'''
# 配置文件信息，设置为全局

from datetime import datetime
import os
import sys

from DP.dp_collcation_cross import *
from DP.dp_prj import prj_core
from app.pb_drc_MERSI_L1 import CLASS_MERSI_L1
from app.pb_drc_MODIS_L1 import CLASS_MODIS_L1


MainPath, MainFile = os.path.split(os.path.realpath(__file__))


class ReadYaml():

    def __init__(self, inFile):
        """
        读取yaml格式配置文件
        """
        if not os.path.isfile(inFile):
            print 'Not Found %s' % inFile
            sys.exit(-1)

        with open(inFile, 'r') as stream:
            cfg = yaml.load(stream)
        self.sat1 = cfg['INFO']['sat1']
        self.sensor1 = cfg['INFO']['sensor1']
        self.sat2 = cfg['INFO']['sat2']
        self.sensor2 = cfg['INFO']['sensor2']
        self.ymd = cfg['INFO']['ymd']
#         self.hms = cfg['INFO']['hms']

        self.ifile1 = cfg['PATH']['ipath1']
        self.ifile2 = cfg['PATH']['ipath2']
        self.ofile = cfg['PATH']['opath']

        self.cmd = cfg['PROJ']['cmd']
        self.col = cfg['PROJ']['col']
        self.row = cfg['PROJ']['row']
        self.res = cfg['PROJ']['res']


def main(inYamlFile):
    T1 = datetime.now()

    # 01 ICFG = 输入配置文件类 ##########
    ICFG = ReadYaml(inYamlFile)

    # 02 MCFG = 阈值配置文件类
    modeFile = os.path.join(
        MainPath, 'cfg', '%s+%s_%s+%s.colloc' % (ICFG.sat1, ICFG.sensor1, ICFG.sat2, ICFG.sensor2))
    MCFG = ReadModeYaml(modeFile)
    if not MCFG.rewrite:
        print 'skip'
        return
    # DCLC = DATA DCLC 匹配结果类
    DCLC = COLLOC_COMM(ICFG.row, ICFG.col, MCFG.chan1)

    T2 = datetime.now()
    print 'read config:', (T2 - T1).total_seconds()

    # 判断是否重写
    if os.path.isfile(ICFG.ofile):
        rewrite_mask = True
    else:
        rewrite_mask = False

    if not rewrite_mask:
        T1 = datetime.now()
        # 03 解析 第一颗传感器的L1数据 ##########
        for inFile in ICFG.ifile1:
            if 'MERSI' == ICFG.sensor1 and 'FY3C' in ICFG.sat1:
                D1 = CLASS_MERSI_L1()
                D1.Load(inFile)
            elif 'VIRR' == ICFG.sensor1 and 'FY3C' in ICFG.sat1:
                D1 = CLASS_VIRR_L1()
                D1.Load(inFile)
            elif 'IRAS' == ICFG.sensor1 and 'FY3C' in ICFG.sat1:
                D1 = CLASS_IRAS_L1()
                D1.Load(inFile)
            elif 'MERSI' == ICFG.sensor1 and 'FY3D' in ICFG.sat1:
                D1 = CLASS_MERSI2_L1()
                LutFile = ''
                D1.LutFile = LutFile
                D1.Load(inFile)
            elif 'AHI' == ICFG.sensor1:
                D1 = CLASS_HMW8_L1()
                D1.Load(inFile)
                geoFile = 'fygatNAV.Himawari08.xxxxxxx.000001_minmin.hdf'
                D1.Loadgeo(geoFile)
            else:
                print 'sensor1:%s not support' % ICFG.sensor1

            # 04 投影，简历查找表  ##########
            print ICFG.cmd
            P1 = prj_core(ICFG.cmd, ICFG.res, row=ICFG.row, col=ICFG.col)
            P1.create_lut(D1.Lons, D1.Lats)
            P1.grid_lonslats()

            # 05 解析 第二颗传感器的L1数据   ##########
            for inFile2 in ICFG.ifile2:
                if 'MODIS' == ICFG.sensor2:
                    D2 = CLASS_MODIS_L1()
                    D2.Load(inFile2)

                elif 'IASI' == ICFG.sensor2:
                    D2 = CLASS_IASI_L1(MCFG.chan1)
                    D2.Load(inFile2)
                    D2.get_rad_tbb(D1, MCFG.chan1)

                elif 'GOME' == ICFG.sensor2:
                    D2 = CLASS_GOME_L1(MCFG.chan1)
                    D2.Load(inFile2)
                    D2.get_rad_tbb(D1, MCFG.chan1)

                elif 'VIIRS' == ICFG.sensor2:
                    D2 = CLASS_VIIRS_L1()
                    D2.Load(inFile2)

                elif 'CRIS' == ICFG.sensor2:
                    if 'SCRIF' in inFile2:
                        print '全分辨率 cris'
                        D2 = CLASS_CRIS_L1(MCFG.chan1)
                        D2.LoadFull(inFile2)
                        D2.gapFilling()
                        D2.get_rad_tbb(D1, MCFG.chan1)
                    else:
                        print '低辨率 cris'
                        D2 = CLASS_CRIS_L1(MCFG.chan1)
                        D2.Load(inFile2)
                        D2.get_rad_tbb(D1, MCFG.chan1)

                else:
                    print 'sensor1:%s not support' % ICFG.sensor2

                # 06 投影，简历查找表  ##########
                P2 = prj_core(ICFG.cmd, ICFG.res, row=ICFG.row, col=ICFG.col)
                P2.create_lut(D2.Lons, D2.Lats)
                # 07 粗匹配 ##########
                DCLC.save_rough_data(P1, P2, D1, D2, MCFG)

        T2 = datetime.now()
        print 'rough:', (T2 - T1).total_seconds()
    else:
        T1 = datetime.now()
        DCLC.reload_data(ICFG, MCFG)
        T2 = datetime.now()
        print 'reload:', (T2 - T1).total_seconds()
    # 08 精匹配  和订正可见光通道的ref值 ##########
    T1 = datetime.now()
    DCLC.save_fine_data(MCFG)
    DCLC.correct_target_ref_data()  # 最后一步 必须放在最后
    T2 = datetime.now()
    print 'colloc:', (T2 - T1).total_seconds()

    # 09 输出匹配结果 ##########
    if rewrite_mask:
        T1 = datetime.now()
        DCLC.rewrite_hdf5(ICFG, MCFG)
        T2 = datetime.now()
        print 'rewrite:', (T2 - T1).total_seconds()
    elif MCFG.rewrite:
        T1 = datetime.now()
        DCLC.write_hdf5(ICFG, MCFG)
        T2 = datetime.now()
        print 'write:', (T2 - T1).total_seconds()

    # 10 对结果进行绘图 ##########
    if MCFG.drawmap:
        T1 = datetime.now()
        DCLC.draw_dclc(ICFG, MCFG)
        T2 = datetime.now()
        print 'map:', (T2 - T1).total_seconds()


if __name__ == '__main__':

    # 获取python输入参数，进行处理
    args = sys.argv[1:]
    if len(args) == 1:  # 跟参数，则处理输入的时段数据
        inYamlFile = args[0]
    else:
        print 'input args error exit'
        sys.exit(-1)

    # 统计整体运行时间
    T_all_1 = datetime.now()
    main(inYamlFile)
    T_all_2 = datetime.now()
    print 'times:', (T_all_2 - T_all_1).total_seconds()

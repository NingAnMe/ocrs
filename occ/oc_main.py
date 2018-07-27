# coding:utf-8

from datetime import datetime
from multiprocessing import Pool, Lock
from time import ctime
import calendar
import getopt
import os
import re
import shutil
import subprocess
import sys
import time

from configobj import ConfigObj
from dateutil.relativedelta import relativedelta
import yaml

from PB import pb_time, pb_io, pb_name
from PB.CSC.pb_csc_console import SocketServer, LogServer


__description__ = u'交叉主调度处理的函数'
__author__ = 'wangpeng'
__date__ = '2018-05-30'
__version__ = '1.0.0_beat'
__updated__ = '2018-07-27'


# python = 'python2.7  -W ignore'
python = 'python2.7'
mpi_run = 'mpirun'
mpi_main = 'mpi.py'
np = 56

# 启动socket服务,防止多实例运行
port = 10000
sserver = SocketServer()
if sserver.createSocket(port) == False:
    sserver.closeSocket(port)
    print (u'----已经有一个实例在实行')
    sys.exit(-1)

# 获取配置文件，手动更改，放到全局
main_path, main_file = os.path.split(os.path.realpath(__file__))
cfg_file = os.path.join(main_path, 'cfg/occ.cfg')
cfg_body = ConfigObj(cfg_file)
LogPath = cfg_body['PATH']['OUT']['log']
CROSS_DIR = cfg_body['PATH']['IN']['cross']
Log = LogServer(LogPath)


def usage():
    print(u"""
    -h / --help :使用帮助
    -v / --verson: 显示版本号
    -j / --job : 作业步骤 -j 01 or --job 01
    -s / --sat : 卫星信息  -s FY3B+MERSI_AQUA+MODIS or --sat FY3B+MERSI_AQUA+MODIS
    -t / --time :日期   -t 20180101-20180101 or --time 20180101-20180101
    """)


def CreateYamlCfg(yaml_dict, cfgFile):
    cfgPath = os.path.dirname(cfgFile)
    if not os.path.isdir(cfgPath):
        os.makedirs(cfgPath)
    with open(cfgFile, 'w') as stream:
        yaml.dump(yaml_dict, stream, default_flow_style=False)


def main():

    try:
        opts, _ = getopt.getopt(
            sys.argv[1:], "hv:j:s:t:", ["version", "help", "job=", "sat=" "time="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err)  # will print something like "option -a not recognized"
        usage()
        sys.exit(1)

    for key, val in opts:
        if key in ('-v', '--version'):
            verbose = '1.0.1'
            print 'Version: %s' % verbose
            sys.exit()

        elif key in ("-h", "--help"):
            usage()
            sys.exit()

        elif key in ("-j", "--job"):
            job_id = val

        elif key in ("-s", "--sat"):

            sat_pair = val

        elif key in ("-t", "--time"):
            str_time = val
        else:
            assert False, "unhandled option"

    rolldays = cfg_body['CROND']['rolldays']

    # 自动或手动的时间处理
    if 'AUTO' in str_time:
        datelist = []
        for rdays in rolldays:
            date_s = (datetime.utcnow() - relativedelta(days=int(rdays)))
            datelist.append(date_s)

        date_list = zip(datelist, datelist)

    else:
        date_s, date_e = pb_time.arg_str2date(str_time)
        date_list = zip([date_s], [date_e])

    # 自动或是手动卫星的处理,默认书写顺序, 有依赖关系时按顺序书写
    if 'ALL' in sat_pair:
        sat_pair_list = cfg_body['PAIRS'].keys()
    else:
        sat_pair_list = [sat_pair]

    # 卫星对
    for sat_pair in sat_pair_list:

        # 获取该卫星对作业流名称,没写则不做
        job_flow_name = cfg_body['PAIRS'][sat_pair]['job_flow']
        if len(job_flow_name) == 0:
            continue

        # 根据自动或是手动的作业流的处理
        if 'ALL' in job_id:
            # 根据作业流获取作业流ID
            job_flow_id_list = cfg_body['JOB_FLOW_DEF'][job_flow_name]
        else:
            job_flow_id_list = ['job_%s' % job_id]

        # 分解作业步
        for job_id_name in job_flow_id_list:
            # 根据命令行输入的作业id获取函数模块名字
            job_mode = cfg_body['BAND_JOB_MODE'][job_id_name]
            job_mode = os.path.join(main_path, job_mode)
            # 多个时间段 依次处理
            for date_s, date_e in date_list:
                # 获取作业需要的参数列表
                arg_list = eval(job_id_name)(
                    job_mode, sat_pair, date_s, date_e, job_id_name)
                # 执行
                run_command_parallel(arg_list)


def job_0110(job_exe, sat_pair, date_s, date_e, job_id):
    """
    ncep处理的输入接口
    """
    Log.info(u'%s: %s ncep处理开始...' % (job_id, job_exe))

    in_path = cfg_body['PATH']['IN']['ncep']
    reg = 'fnl_%s_.*'
    arg_list = []

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        use_in_path = pb_io.path_replace_ymd(in_path, ymd)
        use_reg = reg % ymd
        file_list = pb_io.find_file(use_in_path, use_reg)
        for in_file in file_list:
            cmd_list = '%s %s %s %s' % (python, job_exe, sat_pair, in_file)
            arg_list.append(cmd_list)
        date_s = date_s + relativedelta(days=1)
    return arg_list


def job_0210(job_exe, sat_pair, date_s, date_e, job_id):
    """
    L1数据预处理的输入接口
    """
    Log.info(u'%s: %s L1数据预处理开始...' % (job_id, job_exe))

    in_path = cfg_body['PATH']['IN']['l1']
#     reg = 'FY3[A-Z]_MERSI_GBAL_L1_%s_(\d{4})_1000M_MS.HDF'
    arg_list = []

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        use_in_path = pb_io.path_replace_ymd(in_path, ymd)

        use_reg = '.*_%s_.*.HDF$' % ymd
        file_list = pb_io.find_file(use_in_path, use_reg)
        for in_file in file_list:
            cmd_list = '%s %s %s %s' % (python, job_exe, sat_pair, in_file)
            arg_list.append(cmd_list)
        date_s = date_s + relativedelta(days=1)
    return arg_list


def job_0310(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :反演
    '''
    Log.info(u'%s: %s 反演预处理开始...' % (job_id, job_exe))

    cfg = 'aerosol.cfg'

    # 去掉路径信息
    job_exe = os.path.basename(job_exe)

    in_path = cfg_body['PATH']['MID']['calibrate']
    reg = 'FY3[A-Z]_MERSI_GBAL_L1_%s_(\d{4})_1000M_MS.HDF'
    arg_list = []

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        use_in_path = pb_io.path_replace_ymd(in_path, ymd)
        use_reg = '.*_%s_.*.HDF' % ymd
        file_list = pb_io.find_file(use_in_path, use_reg)
        for in_file in file_list:
            cmd_list = './%s %s %s ' % (job_exe, in_file, cfg)
            arg_list.append(cmd_list)

        date_s = date_s + relativedelta(days=1)

    return arg_list


def job_0311(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :水色反演
    '''
    Log.info(u'%s: %s 水色反演处理开始...' % (job_id, job_exe))

    # 去掉路径信息

    in_path = cfg_body['PATH']['MID']['granule']
    out_path = cfg_body['PATH']['OUT']['occ']
    arg_list = []

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        use_in_path = pb_io.path_replace_ymd(in_path, ymd)
        use_out_path = pb_io.path_replace_ymd(out_path, ymd)
        if not os.path.isdir(use_out_path):
            os.makedirs(use_out_path)
        use_reg = '.*_%s_.*.HDF' % ymd
        file_list = pb_io.find_file(use_in_path, use_reg)
        for in_file in file_list:
            cmd_list = 'idl -rt=%s -args %s %s/' % (
                job_exe, in_file, use_out_path)
            arg_list.append(cmd_list)

        date_s = date_s + relativedelta(days=1)

    return arg_list


def job_0410(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :反演后的快视图
    '''
    Log.info(u'%s: %s 反演轨道产品快视图处理开始...' % (job_id, job_exe))

    in_path = cfg_body['PATH']['MID']['granule']
#     reg = 'FY3[A-Z]_MERSI_ORBT_L2_\w{3}_MLT_NUL_%s_(\d{4})_1000M.HDF'
    arg_list = []

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        use_in_path = pb_io.path_replace_ymd(in_path, ymd)
        use_reg = '.*_%s_.*.HDF' % ymd
        file_list = pb_io.find_file(use_in_path, use_reg)
        for in_file in file_list:
            cmd_list = '%s %s %s %s' % (python, job_exe, sat_pair, in_file)
            arg_list.append(cmd_list)
        date_s = date_s + relativedelta(days=1)

    return arg_list


def job_0510(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :投影
    '''
    Log.info(u'%s: %s 投影处理开始...' % (job_id, job_exe))
    return job_0410(job_exe, sat_pair, date_s, date_e, job_id)


def job_0610(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :日合成
    '''
    Log.info(u'%s: %s 日合成处理开始...' % (job_id, job_exe))
    granule_path = cfg_body['PATH']['MID']['granule']
    proj_path = cfg_body['PATH']['MID']['projection']
    cfg_path = cfg_body['PATH']['MID']['incfg']
    daily_path = cfg_body['PATH']['OUT']['daily']

    # 清理配置
    if os.path.isdir(cfg_path):
        shutil.rmtree(cfg_path)
    arg_list = []
    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')

        com_filename = '%s_%s_GBAL_L3_OCC_MLT_GLL_%s_AOAD_5000M.HDF' % (
            cfg_body['PATH']['sat'], cfg_body['PATH']['sensor'], ymd)

        daily_path_use = pb_io.path_replace_ymd(daily_path, ymd)
        com_out_file = os.path.join(daily_path_use, com_filename)

        granule_path_use = pb_io.path_replace_ymd(granule_path, ymd)
        reg = '.*_%s_.*.HDF' % ymd
        granule_lst = pb_io.find_file(granule_path_use, reg)

#         reg = '.*_%s_.*.HDF' % ymd
        proj_path_use = pb_io.path_replace_ymd(proj_path, ymd)
        granule_pro_lst = pb_io.find_file(proj_path_use, reg)

        if len(granule_pro_lst) > 0:
            com_dict = {
                'PATH': {'ipath': granule_lst, 'ppath': granule_pro_lst, 'opath': com_out_file}}
            cfgFile = os.path.join(cfg_path, '%s.yaml' % ymd)
            CreateYamlCfg(com_dict, cfgFile)

        date_s = date_s + relativedelta(days=1)

    reg = '.*.yaml'
    FileLst = pb_io.find_file(cfg_path, reg)
    for in_file in FileLst:
        cmd_list = '%s %s %s %s' % (python, job_exe, sat_pair, in_file)
        arg_list.append(cmd_list)
    return arg_list


def job_0611(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :合成后文件绘图
    '''
    Log.info(u'%s: %s 日合成出图处理开始...' % (job_id, job_exe))
    ipath = cfg_body['PATH']['OUT']['daily']
    FileLst = []
    arg_list = []
    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        path_use = pb_io.path_replace_ymd(ipath, ymd)
        reg = '.*_%s_.*.HDF' % ymd
        dlist = pb_io.find_file(path_use, reg)
        FileLst.extend(dlist)
        date_s = date_s + relativedelta(days=1)

    for in_file in FileLst:
        cmd_list = '%s %s %s %s' % (python, job_exe, sat_pair, in_file)
        arg_list.append(cmd_list)

    return arg_list


def job_0710(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :月合成
    '''
    Log.info(u'%s: %s 月合成处理开始...' % (job_id, job_exe))
    cfg_path = cfg_body['PATH']['MID']['incfg']
    daily_path = cfg_body['PATH']['OUT']['daily']
    month_path = cfg_body['PATH']['OUT']['monthly']

    # 清理配置
    if os.path.isdir(cfg_path):
        shutil.rmtree(cfg_path)
    arg_list = []

    # 月首和月末 调整
    ymd1 = date_s.strftime('%Y%m%d')
    ymd2 = date_e.strftime('%Y%m%d')
    lastday = calendar.monthrange(int(ymd2[:4]), int(ymd2[4:6]))[1]
    date_s = datetime.strptime('%s01' % ymd1[0:6], '%Y%m%d')
    date_e = datetime.strptime('%s%d' % (ymd2[0:6], lastday), '%Y%m%d')

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')

        # 输出
        com_filename = '%s_%s_GBAL_L3_OCC_MLT_GLL_%s_AOAM_5000M.HDF' % (
            cfg_body['PATH']['sat'], cfg_body['PATH']['sensor'], ymd)

        month_path_use = pb_io.path_replace_ymd(month_path, ymd)
        com_out_file = os.path.join(month_path_use, com_filename)

        # 输入
        reg = '.*_%s.*.HDF' % ymd[0:6]
        daily_path_use = pb_io.path_replace_ymd(daily_path, ymd)
        data_list_use = pb_io.find_file(daily_path_use, reg)

        if len(data_list_use) > 0:
            com_dict = {
                'PATH': {'ipath': data_list_use, 'opath': com_out_file}}
            cfgFile = os.path.join(cfg_path, '%s.yaml' % ymd)
            CreateYamlCfg(com_dict, cfgFile)

        date_s = date_s + relativedelta(months=1)

    reg = '.*.yaml'
    FileLst = pb_io.find_file(cfg_path, reg)
    for in_file in FileLst:
        cmd_list = '%s %s %s %s' % (python, job_exe, sat_pair, in_file)
        arg_list.append(cmd_list)
    return arg_list


def job_0711(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :月成后文件绘图
    '''
    Log.info(u'%s: %s 月合成出图处理开始...' % (job_id, job_exe))

    ipath = cfg_body['PATH']['OUT']['monthly']
    FileLst = []
    arg_list = []

    # 月首和月末 调整
    ymd1 = date_s.strftime('%Y%m%d')
    ymd2 = date_e.strftime('%Y%m%d')
    lastday = calendar.monthrange(int(ymd2[:4]), int(ymd2[4:6]))[1]
    date_s = datetime.strptime('%s01' % ymd1[0:6], '%Y%m%d')
    date_e = datetime.strptime('%s%d' % (ymd2[0:6], lastday), '%Y%m%d')

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        path_use = pb_io.path_replace_ymd(ipath, ymd)
        reg = '.*_%s_.*.HDF' % ymd
        mlist = pb_io.find_file(path_use, reg)
        FileLst.extend(mlist)
        date_s = date_s + relativedelta(months=1)

    for in_file in FileLst:
        cmd_list = '%s %s %s %s' % (python, job_exe, sat_pair, in_file)
        arg_list.append(cmd_list)

    return arg_list


def job_0211(job_exe, sat_pair, date_s, date_e, job_id):

    Log.info(u'%s: %s 交叉匹配处理开始...' % (job_id, job_exe))

    # 解析mathcing: FY3A+MERSI_AQUA+MODIS ,根据下划线分割获取 卫星+传感器 ,再次分割获取俩颗卫星短名
    sat1 = (sat_pair.split('_')[0]).split('+')[0]
    sensor1 = (sat_pair.split('_')[0]).split('+')[1]
    sat2 = (sat_pair.split('_')[1]).split('+')[0]
    sensor2 = (sat_pair.split('_')[1]).split('+')[1]
    # 解析global.cfg中的信息
    sec1 = cfg_body['PAIRS'][sat_pair]['sec1']
    sec2 = cfg_body['PAIRS'][sat_pair]['sec2']

    DATA_DIR = cfg_body['PATH']['IN']['data']
    jobCfg = cfg_body['PATH']['IN']['jobCfg']
    match_path = cfg_body['PATH']['MID']['match']

    # 存放分发列表
    arg_list = []

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        jjj = date_s.strftime('%j')

        print ymd
        # 存放俩颗卫星的原始数据目录位置
        inpath1 = os.path.join(DATA_DIR, '%s/%s/L1/ORBIT' %
                               (sat1, sensor1), ymd[:6])
        inpath2 = os.path.join(DATA_DIR, '%s/%s/L1/ORBIT' %
                               (sat2, sensor2, ), ymd[:6])

        sat11 = cfg_body['SAT_S2L'][sat1]
        sat22 = cfg_body['SAT_S2L'][sat2]

        # 读取交叉点上的俩颗卫星的交叉时间，1列=经度  2列=纬度  3列=卫星1时间  4列=卫星2时间
        timeList = ReadCrossFile_LEO_LEO(sat11, sat22, ymd)
        print 'cross', len(timeList)
        reg1 = 'FY3B_MERSI.*_%s_.*.HDF' % ymd
        reg2 = 'MYD021KM.A%s%s.*.hdf' % (ymd[0:4], jjj)
        file_list1 = pb_io.find_file(inpath1, reg1)
        file_list2 = pb_io.find_file(inpath2, reg2)

        # 根据交叉点时间，找到数据列表中需要的数据 select File
        for crossTime in timeList:
            Lat = crossTime[0]
            Lon = crossTime[1]
            ymdhms = crossTime[2].strftime('%Y%m%d%H%M%S')
            s_cross_time1 = crossTime[2] - relativedelta(seconds=int(sec1))
            e_cross_time1 = crossTime[2] + relativedelta(seconds=int(sec1))
            s_cross_time2 = crossTime[3] - relativedelta(seconds=int(sec2))
            e_cross_time2 = crossTime[3] + relativedelta(seconds=int(sec2))

            # 从数据列表中查找过此交叉点时间的数据块,两颗卫星的数据
            list1 = Find_data_FromCrossTime(
                file_list1, s_cross_time1, e_cross_time1)
            list2 = Find_data_FromCrossTime(
                file_list2, s_cross_time2, e_cross_time2)
            print 'fy', len(list1)
            print 'mo', len(list2)
            # 存放匹配信息的yaml配置文件存放位置

            yaml_file3 = os.path.join(
                jobCfg, sat_pair, job_id, ymdhms[:8], '%s_%s_%s.yaml' % (ymdhms, sensor1, sensor2))

            filename3 = '%s_MATCHEDPOINTS_%s.H5' % (sat_pair, ymdhms)

            # 输出完整路径
            full_filename3 = os.path.join(
                match_path, sat_pair, ymdhms[:6], filename3)

            # 投影参数
            cmd = '+proj=laea  +lat_0=%f +lon_0=%f +x_0=0 +y_0=0 +ellps=WGS84' % (
                Lat, Lon)

            if len(list1) > 0 and len(list2) > 0:
                print '111111'
                row = 128
                col = 128
                res = 8000

                dict3 = {'INFO': {'sat1': sat1, 'sensor1': sensor1, 'sat2': sat2, 'sensor2': sensor2, 'ymd': ymdhms},
                         'PATH': {'opath': full_filename3, 'ipath1': list1, 'ipath2': list2},
                         'PROJ': {'cmd': cmd, 'row': row, 'col': col, 'res': res}}

                Log.info('%s %s create collocation cfg success' %
                         (sat_pair, ymdhms))
                CreateYamlCfg(dict3, yaml_file3)
                cmd = '%s %s %s' % (python, job_exe, yaml_file3)
                arg_list.append(cmd)

        date_s = date_s + relativedelta(days=1)

#     # 开始遍历yaml
#     yaml_path = os.path.join(jobCfg, sat_pair, job_id)
#     yaml_list = pb_io.find_file(yaml_path, '.*.yaml')
#     for in_file in yaml_list:
#         cmd = '%s %s %s' % (python, job_exe, in_file)
#         arg_list.append(cmd)

    return arg_list


def job_0212(job_exe, sat_pair, date_s, date_e, job_id):

    Log.info(u'%s: %s 交叉匹配结果绘图处理开始...' % (job_id, job_exe))

    # 解析mathcing: FY3A+MERSI_AQUA+MODIS ,根据下划线分割获取 卫星+传感器 ,再次分割获取俩颗卫星短名
    sat1 = (sat_pair.split('_')[0]).split('+')[0]
    sensor1 = (sat_pair.split('_')[0]).split('+')[1]
    sat2 = (sat_pair.split('_')[1]).split('+')[0]
    sensor2 = (sat_pair.split('_')[1]).split('+')[1]

    cfg_path = cfg_body['PATH']['MID']['incfg']
    match_path = cfg_body['PATH']['MID']['match']

    # 清理配置
    if os.path.isdir(cfg_path):
        shutil.rmtree(cfg_path)

    # 存放分发列表
    arg_list = []
    all_file_list = []
    stime = date_s.strftime('%Y%m%d')
    etime = date_e.strftime('%Y%m%d')
    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')

        use_match_path = os.path.join(match_path, sat_pair, ymd[0:6])
        reg1 = '.*_%s.*.H5' % ymd
        print use_match_path
        file_list = pb_io.find_file(use_match_path, reg1)
        all_file_list.extend(file_list)
        date_s = date_s + relativedelta(days=1)

    ofile_yaml = os.path.join(cfg_path, '%s_%s.yaml' % (stime, etime))
    opath = os.path.join(
        match_path, '%s_map' % sat_pair, '%s_%s' % (stime, etime))

    if not os.path.isdir(opath):
        os.makedirs(opath)

    dict = {'INFO': {'sat1': sat1, 'sensor1': sensor1, 'sat2': sat2, 'sensor2': sensor2, 'ymd_s': stime, 'ymd_e': etime},
            'PATH': {'opath': opath, 'ipath': all_file_list}}

    if len(all_file_list) > 0:
        Log.info('%s %s-%s create collocation cfg success' %
                 (sat_pair, stime, etime))
        CreateYamlCfg(dict, ofile_yaml)
        cmd = '%s %s %s %s' % (python, job_exe, sat_pair, ofile_yaml)
        arg_list.append(cmd)

    return arg_list


def job_0213(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    python2.7 oc_main.py  -s FY3B+MERSI_AQUA+MODIS -j 0213 -t 20130101-2013033
    '''
    Log.info(u'%s: %s 交叉匹配结果长时间图处理开始...' % (job_id, job_exe))
    return job_0212(job_exe, sat_pair, date_s, date_e, job_id)


def ReadCrossFile_LEO_LEO(sat1, sat2, ymd):

    # 拼接cross, snox预报文件
    Filedir = sat1 + '_' + sat2
    FileName1 = Filedir + '_' + ymd + '.txt'
    crossFile = os.path.join(CROSS_DIR, Filedir, FileName1)
    index1 = (1, 2, 3, 4)

    Lines1 = []
    # 交叉点预报文件内容
    if os.path.isfile(crossFile):
        fp = open(crossFile, 'r')
        bufs = fp.readlines()
        fp.close()
        # 获取长度不包含头信息
        Lines1 = bufs[10:]

    timelst1 = get_cross_file_timelist(Lines1, index1)
    timeList = timelst1

    return timeList


def get_cross_file_timelist(Lines, index):

    # 获取交叉匹配文件中的信息
    timeList = []
    for Line in Lines:
        ymd = Line.split()[0].strip()
        hms1 = Line.split()[index[0]].strip()
        lat1 = float(Line.split()[index[1]].strip())
        lon1 = float(Line.split()[index[2]].strip())
        hms2 = Line.split()[index[3]].strip()
        cross_time1 = datetime.strptime(
            '%s %s' % (ymd, hms1), '%Y%m%d %H:%M:%S')
        cross_time2 = datetime.strptime(
            '%s %s' % (ymd, hms2), '%Y%m%d %H:%M:%S')
        timeList.append([lat1, lon1, cross_time1, cross_time2])

    return timeList


def Find_data_FromCrossTime(FileList, start_crossTime, end_crossTime):
    dataList = []
    for FileName in FileList:
        name = os.path.basename(FileName)
        nameClass = pb_name.nameClassManager()
        info = nameClass.getInstance(name)
        if info is None:
            continue
        # 获取数据时间段
        data_stime1 = info.dt_s
        data_etime1 = info.dt_e
        if InCrossTime(data_stime1, data_etime1, start_crossTime, end_crossTime):
            dataList.append(FileName)
    return dataList


def InCrossTime(s_ymdhms1, e_ymdhms1, s_ymdhms2, e_ymdhms2):
    '''
    判断俩个时间段是否有交叉
    '''

    if s_ymdhms2 <= s_ymdhms1 < e_ymdhms2:
        return True
    elif s_ymdhms2 < e_ymdhms1 <= e_ymdhms2:
        return True
    elif s_ymdhms2 > s_ymdhms1 and e_ymdhms2 < e_ymdhms1:
        return True
    else:
        return False


def run_command_parallel(arg_list):

    arg_list = [each + '\n' for each in arg_list]
    fp = open('filelist.txt', 'w')
    fp.writelines(arg_list)
    fp.close()

    cmd = '%s -np %d -machinefile hostfile %s %s' % (
        mpi_run, np, python, mpi_main)
    os.system(cmd)


def run_command(arg_list):
    Log.info(u'in function run_command')
    # 开启进程池
    threadNum = cfg_body['CROND']['threads']

    if len(arg_list) > 0:
        pool = Pool(processes=int(threadNum))
        for cmd_list in arg_list:
            pool.apply_async(command, (cmd_list,))
        pool.close()
        pool.join()


def command(args_cmd):
    '''
    args_cmd: python a.py 20180101  (完整的执行参数)
    '''

    print args_cmd
    try:
        P1 = subprocess.Popen(args_cmd.split())
    except Exception, e:
        Log.error(e)
        return

    timeout = 6000
    t_beginning = time.time()
    seconds_passed = 0

    while (P1.poll() is None):

        seconds_passed = time.time() - t_beginning

        if timeout and seconds_passed > timeout:
            print seconds_passed
            P1.kill()
        time.sleep(1)
    P1.wait()


if __name__ == '__main__':
    main()

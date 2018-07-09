# coding:utf-8

from datetime import datetime
from multiprocessing import Pool, Lock
from time import ctime
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

from PB import pb_time, pb_io
from PB.CSC.pb_csc_console import SocketServer, LogServer


__description__ = u'交叉主调度处理的函数'
__author__ = 'wangpeng'
__date__ = '2018-05-30'
__version__ = '1.0.0_beat'
__updated__ = '2018-07-09'


python = 'python2.7  -W ignore'
mpi_run = 'mpirun'
mpi_main = 'mpi_main.py'
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

            print '111', job_id_name, job_mode
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
    Log.info(u'get arg list from %s' % job_id)

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
    Log.info(u'get arg list from %s' % job_id)

    in_path = cfg_body['PATH']['IN']['l1']
    reg = 'FY3[A-Z]_MERSI_GBAL_L1_%s_(\d{4})_1000M_MS.HDF'
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


def job_0310(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :反演
    '''
    Log.info(u'get arg list from %s' % job_id)
    cfg = 'aerosol.cfg'

    # 去掉路径信息
    job_exe = os.path.basename(job_exe)

    in_path = cfg_body['PATH']['MID']['calibrate']
    reg = 'FY3[A-Z]_MERSI_GBAL_L1_%s_(\d{4})_1000M_MS.HDF'
    arg_list = []

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        use_in_path = pb_io.path_replace_ymd(in_path, ymd)
        use_reg = reg % ymd
        file_list = pb_io.find_file(use_in_path, use_reg)
        for in_file in file_list:
            cmd_list = './%s %s %s ' % (job_exe, in_file, cfg)
            arg_list.append(cmd_list)

        date_s = date_s + relativedelta(days=1)

    return arg_list


def job_0410(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :反演后的快视图
    '''
    Log.info(u'get arg list from %s' % job_id)

    in_path = cfg_body['PATH']['MID']['granule']
    reg = 'FY3[A-Z]_MERSI_ORBT_L2_\w{3}_MLT_NUL_%s_(\d{4})_1000M.HDF'
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


def job_0510(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :投影
    '''
    Log.info(u'get arg list from %s' % job_id)
    return job_0410(job_exe, sat_pair, date_s, date_e, job_id)


def job_0610(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :日合成
    '''
    Log.info(u'get arg list from %s' % job_id)
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
        reg = 'FY3[A-Z]_MERSI_ORBT_L2_OCC_MLT_NUL_%s_(\d{4})_1000M.HDF' % ymd
        granule_lst = pb_io.find_file(granule_path_use, reg)

        reg = 'FY3[A-Z]_MERSI_ORBT_L2_\w{3}_MLT_NUL_%s_(\d{4})_5000M.HDF' % ymd
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


def job_0710(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :月合成
    '''
    Log.info(u'get arg list from %s' % job_id)
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
        reg = 'FY3[A-Z]_MERSI_ORBT_L2_\w{3}_MLT_NUL_%s.*_(\d{4})_5000M.HDF' % ymd[
            0:6]
        daily_path_use = pb_io.path_replace_ymd(daily_path, ymd)
        data_list_use = pb_io.find_file(daily_path_use, reg)

        if len(granule_pro_lst) > 0:
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


def job_0810(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :合成后文件绘图
    '''
    Log.info(u'get arg list from %s' % job_id)

    daily_path = cfg_body['PATH']['OUT']['daily']
    FileLst = []
    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        timeStep = relativedelta(days=1)
        daily_path_use = pb_io.path_replace_ymd(daily_path, ymd)
        com_filename = '%s_%s_GBAL_L3_\w{3}_MLT_GLL_%s_AOAD_5000M.HDF' % (
            cfg_body['PATH']['sat'], cfg_body['PATH']['sensor'], ymd)
        FileLst.append(os.path.join(daily_path_use, com_filename))
        date_s = date_s + timeStep

    for in_file in FileLst:
        cmd_list = '%s %s %s %s' % (python, job_exe, sat_pair, in_file)
        arg_list.append(cmd_list)

    return arg_list


def run_command_parallel(arg_list):

    arg_list = [each + '\n' for each in arg_list]
    fp = open('filelist.txt', 'w')
    fp.writelines(arg_list)
    fp.close()

    cmd = '%s -np %d -machinefile hostfile %s %s' % (
        mpi_run, np, python, mpi_main)
    print cmd
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


def get_arglist_job01_04(job_exe, sat_pair, date_s, date_e, job_id):

    arg_list = []
    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        cfg_path = os.path.join(
            JOBCFG_DIR, sat_pair, job_id, ymd)

        if not os.path.isdir(cfg_path):
            Log.error(u'not found %s ' % (cfg_path))
            date_s = date_s + relativedelta(days=1)
            continue

        Lst = sorted(os.listdir(cfg_path), reverse=False)

        for Line in Lst:
            yaml_file = os.path.join(cfg_path, Line)
            cmd_list = '%s %s %s' % (python, job_exe, yaml_file)
            arg_list.append(cmd_list)

        date_s = date_s + relativedelta(days=1)

    return arg_list


def create_hostfile(mode=0):

    hostfile = 'hostfile'
    if mode == 0:
        # 预处理由于网盘原因暂时用主控开并行
        hostname = ['cluster973\n'] * 10
        fp = open(hostfile, 'w')
        fp.writelines(hostname)
        fp.close()
    elif mode == 1:
        hostname = ['compute-0-0\n', 'compute-0-1\n', 'compute-0-2\n', 'compute-0-3\n',
                    'compute-0-4\n', 'compute-0-5\n', 'compute-0-6\n', 'compute-0-7\n'] * 7
        fp = open(hostfile, 'w')
        fp.writelines(hostname)
        fp.close()

    npnums = len(hostname)
    return npnums

if __name__ == '__main__':
    #     create_hostfile(1)
    main()

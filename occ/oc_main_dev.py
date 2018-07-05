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
__updated__ = '2018-06-27'


python = '/share/apps/soft/python_All/python2.7.15/bin/python27'
mpiRun = '/usr/mpi/intel/mvapich-1.2.0/bin/mpirun_rsh'
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

            print job_id_name, job_mode
            # 多个时间段 依次处理
            for date_s, date_e in date_list:
                # 获取作业需要的参数列表
                arg_list = eval(job_id_name)(
                    job_mode, sat_pair, date_s, date_e, job_id_name)
                # 执行
                if int(job_id) >= 310:
                    print '22222222222222'
                    run_command_parallel(arg_list)
                else:
                    run_command(arg_list)


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
    file_list_all = []

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        use_in_path = pb_io.path_replace_ymd(in_path, ymd)
        use_reg = reg % ymd
        file_list = pb_io.find_file(use_in_path, use_reg)

        file_list_all.extend(file_list)
        date_s = date_s + relativedelta(days=1)

    # 生成filelist
    file_list1 = [each + '\n' for each in file_list_all]
    fp = open('filelist.txt', 'w')
    fp.writelines(file_list1)
    fp.close()
    cmd = '%s -np %s -hostfile hostfile parallel.exe -l \
        ./filelist.txt -e ./%s -c %s' % (mpiRun, '56', job_exe, cfg)

    arg_list.append(cmd)

    return arg_list


def job_0410(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :反演后的快视图
    '''
    Log.info(u'get arg list from %s' % job_id)

    in_path = cfg_body['PATH']['MID']['granule']
    reg = 'FY3[A-Z]_MERSI_ORBT_L2_OCC_MLT_NUL_%s_(\d{4})_1000M.HDF'
    arg_list = []
    file_list_all = []
    job_exe = os.path.basename(job_exe)
    job_exe = '%s\ -W\ ignore\ %s\ %s' % (python, job_exe, sat_pair)

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        use_in_path = pb_io.path_replace_ymd(in_path, ymd)
        use_reg = reg % ymd
        file_list = pb_io.find_file(use_in_path, use_reg)

        file_list_all.extend(file_list)
        date_s = date_s + relativedelta(days=1)

    # 生成filelist
    file_list1 = [each + '\n' for each in file_list_all]
    fp = open('filelist.txt', 'w')
    fp.writelines(file_list1)
    fp.close()
    cmd = '%s -np %s -hostfile hostfile parallel.exe -l \
        ./filelist.txt -e "%s"' % (mpiRun, '56',  job_exe)
    arg_list.append(cmd)
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
    job_exe = os.path.basename(job_exe)
    job_exe = '%s\ -W\ ignore\ %s\ %s' % (python, job_exe, sat_pair)
    # 清理配置
    if os.path.isdir(cfg_path):
        shutil.rmtree(cfg_path)

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')

        com_filename = '%s_%s_GBAL_L3_OCC_MLT_GLL_%s_AOAD_5000M.HDF' % (
            cfg_body['PATH']['sat'], cfg_body['PATH']['sensor'], ymd)

        daily_path_use = pb_io.path_replace_ymd(daily_path, ymd)
        com_out_file = os.path.join(daily_path_use, com_filename)

        granule_path_use = pb_io.path_replace_ymd(granule_path, ymd)
        reg = 'FY3[A-Z]_MERSI_ORBT_L2_OCC_MLT_NUL_%s_(\d{4})_1000M.HDF' % ymd
        granule_lst = pb_io.find_file(granule_path_use, reg)

        reg = 'FY3[A-Z]_MERSI_ORBT_L2_OCC_MLT_NUL_%s_(\d{4})_5000M.HDF' % ymd
        proj_path_use = pb_io.path_replace_ymd(proj_path, ymd)
        granule_pro_lst = pb_io.find_file(proj_path_use, reg)
        print len(granule_pro_lst)

        com_dict = {
            'PATH': {'ipath': granule_lst, 'ppath': granule_pro_lst, 'opath': com_out_file}}
        cfgFile = os.path.join(cfg_path, '%s.yaml' % ymd)
        CreateYamlCfg(com_dict, cfgFile)

        date_s = date_s + relativedelta(days=1)

    reg = '.*.yaml'
    FileLst = pb_io.find_file(cfg_path, reg)
    FileLst = [each + '\n' for each in FileLst]
    fp = open('filelist.txt', 'w')
    fp.writelines(FileLst)
    fp.close()
    arg_list = []
    cmd = '%s -np %s -hostfile hostfile parallel.exe -l \
        ./filelist.txt -e "%s"' % (mpiRun, '56',  job_exe)
    arg_list.append(cmd)
    return arg_list


def job_0810(job_exe, sat_pair, date_s, date_e, job_id):
    Log.info(u'get arg list from %s' % job_id)

    job_exe = os.path.basename(job_exe)
    job_exe = '%s\ -W\ ignore\ %s\ %s' % (python, job_exe, sat_pair)

    daily_path = cfg_body['PATH']['OUT']['daily']
    FileLst = []
    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        timeStep = relativedelta(days=1)
        daily_path_use = pb_io.path_replace_ymd(daily_path, ymd)
        com_filename = '%s_%s_GBAL_L3_OCC_MLT_GLL_%s_AOAD_5000M.HDF' % (
            cfg_body['PATH']['sat'], cfg_body['PATH']['sensor'], ymd)
        FileLst.append(os.path.join(daily_path_use, com_filename))
        date_s = date_s + timeStep

    FileLst = [each + '\n' for each in FileLst]
    fp = open('filelist.txt', 'w')
    fp.writelines(FileLst)
    fp.close()

    arg_list = []
    cmd = '%s -np %s -hostfile hostfile parallel.exe -l \
        ./filelist.txt -e "%s"' % (mpiRun, '56',  job_exe)
    arg_list.append(cmd)
    return arg_list


def run_command_parallel(arg_list):
    for cmd_list in arg_list:
        os.system(cmd_list)


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

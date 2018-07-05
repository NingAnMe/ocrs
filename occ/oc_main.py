# coding=utf-8
import getopt
import os
import re
import shutil
import sys
import warnings

from configobj import ConfigObj
from dateutil.relativedelta import relativedelta
import yaml

from PB import pb_io, pb_time
from PB.CSC.pb_csc_console import SocketServer


warnings.filterwarnings("ignore")

__author__ = 'wangpeng'

'''
调度接口
1、L1预处理
2、水色反演产品
3、水色产品321快视图
4、水色产品投影
5、水色产品日合成
6、水色产品日合成出图
'''

# 启动socket服务,防止多实例运行
port = 10000
sserver = SocketServer()
if sserver.createSocket(port) == False:
    sserver.closeSocket(port)
    print (u'----已经有一个实例在实行')
    sys.exit(-1)

MainPath, MainFile = os.path.split(os.path.realpath(__file__))
cfgFile = os.path.join(MainPath, 'global.cfg')
inCfg = ConfigObj(cfgFile)

python = '/share/apps/soft/python_All/python2.7.15/bin/python27'
npnum = 8
PyExe = {'00': '%s\ -W\ ignore\ oc_00_ncep_to_byte.py' % python,  # ncep转换
         '01': '%s\ -W\ ignore\ oc_01_calibrate.py' % python,  # 预处理
         '02': './oc_02_aerosol.exe',  # 反演
         '03': '%s\ -W\ ignore\ oc_03_quick_view_img.py' % python,  # 快视图
         '04': '%s\ -W\ ignore\ oc_04_projection.py' % python,  # 投影
         '05': '%s\ -W\ ignore\ oc_05_combine_day.py' % python,  # 日合成
         '06': '%s\ -W\ ignore\ oc_06_combine_map.py' % python,  # 日合成分布图
         }


def usage():
    print(u"""
    -h / --help :使用帮助
    -v / --verson: 显示版本号
    -j / --job : 作业步骤 -j 01 or --job 01
    -s / --sat : 卫星信息  -s FY3B+MERSI or --sat FY3B+MERSI
    -t / --time :日期   -t 20180101-20180101 or --time 20180101-20180101
    """)


def main():

    try:
        opts, args = getopt.getopt(
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
            jobID = val

        elif key in ("-s", "--sat"):

            satType = val

        elif key in ("-t", "--time"):
            strTime = val
        else:
            assert False, "unhandled option"

    date_s, date_e = pb_time.arg_str2date(strTime)

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')

        if '00' in jobID:
            print 'start %s calibrate process ......' % jobID
            create_00_ncep(ymd)
            npnums = create_hostfile(0)
            run_parallel(PyExe[jobID], npnums)
            timeStep = relativedelta(months=1)

        elif '01' in jobID:
            print 'start %s calibrate process ......' % jobID
            create_01_calibrate(ymd)
            npnums = create_hostfile(0)
            run_parallel(PyExe[jobID], npnums)
            timeStep = relativedelta(days=1)

        elif '02' in jobID:
            print 'start %s aerosol process ......' % jobID
            create_02_aerosol(ymd)
            npnums = create_hostfile(1)
            run_parallel(PyExe[jobID], npnums, 'aerosol.cfg')
            timeStep = relativedelta(days=1)

        elif '03' in jobID:
            print 'start %s ture color precess ......' % jobID
            create_03_quick_view(ymd)
            npnums = create_hostfile(1)
            run_parallel(PyExe[jobID], npnums)
            timeStep = relativedelta(days=1)

        elif '04' in jobID:
            print 'start %s projection precess ......' % jobID
            create_04_project(ymd)
            npnums = create_hostfile(1)
            run_parallel(PyExe[jobID], npnums)
            timeStep = relativedelta(days=1)

        elif '05' in jobID:
            print 'start %s combine precess ......' % jobID
            create_05_combine_d(date_s, date_e)
            npnums = create_hostfile(1)
            run_parallel(PyExe[jobID], npnums)
            break
        elif '06' in jobID:
            print 'start %s projection precess ......' % jobID
            create_06_combine_d_map(date_s, date_e)
            npnums = create_hostfile(1)
            run_parallel(PyExe[jobID], npnums)
            break

        date_s = date_s + timeStep


def create_00_ncep(ymd):

    ncep = inCfg['PATH']['IN']['ncep']
    ncep = pb_io.path_replace_ymd(ncep, ymd)
    reg = 'fnl.*'
    FileLst = find_file(ncep, reg)
    FileLst = [each + '\n' for each in FileLst]
    fp = open('filelist.txt', 'w')
    fp.writelines(FileLst)
    fp.close()


def create_01_calibrate(ymd):

    L1Path = inCfg['PATH']['IN']['l1']
    L1Path = pb_io.path_replace_ymd(L1Path, ymd)
    reg = 'FY3[A-Z]_MERSI_GBAL_L1_%s_(\d{4})_1000M_MS.HDF' % ymd
    FileLst = find_file(L1Path, reg)
    FileLst = [each + '\n' for each in FileLst]
    fp = open('filelist.txt', 'w')
    fp.writelines(FileLst)
    fp.close()


def create_02_aerosol(ymd):

    calibrate = inCfg['PATH']['MID']['calibrate']
    calibrate = pb_io.path_replace_ymd(calibrate, ymd)
    reg = 'FY3[A-Z]_MERSI_GBAL_L1_%s_(\d{4})_1000M_MS.HDF' % ymd
    FileLst = find_file(calibrate, reg)
    FileLst = [each + '\n' for each in FileLst]
    fp = open('filelist.txt', 'w')
    fp.writelines(FileLst)
    fp.close()


def create_03_quick_view(ymd):

    granule = inCfg['PATH']['MID']['granule']
    granule = pb_io.path_replace_ymd(granule, ymd)
    reg = 'FY3[A-Z]_MERSI_ORBT_L2_ASO_MLT_NUL_%s_(\d{4})_1000M.HDF' % ymd
    FileLst = find_file(granule, reg)
    FileLst = [each + '\n' for each in FileLst]
    fp = open('filelist.txt', 'w')
    fp.writelines(FileLst)
    fp.close()


def create_04_project(ymd):

    granule = inCfg['PATH']['MID']['granule']
    granule = pb_io.path_replace_ymd(granule, ymd)
    # 反演的轨道产品清单
    reg = 'FY3[A-Z]_MERSI_ORBT_L2_ASO_MLT_NUL_%s_(\d{4})_1000M.HDF' % ymd
    FileLst = find_file(granule, reg)
    FileLst = [each + '\n' for each in FileLst]

    fp = open('filelist.txt', 'w')
    fp.writelines(FileLst)
    fp.close()


def create_05_combine_d(date_s, date_e):

    granule_path = inCfg['PATH']['MID']['granule']
    proj_path = inCfg['PATH']['MID']['projection']
    cfg_path = inCfg['PATH']['MID']['incfg']
    daily_path = inCfg['PATH']['OUT']['daily']

    # 清理配置
    if os.path.isdir(cfg_path):
        shutil.rmtree(cfg_path)

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')

        com_filename = '%s_%s_GBAL_L3_ASO_MLT_GLL_%s_AOAD_5000M.HDF' % (
            inCfg['PATH']['sat'], inCfg['PATH']['sensor'], ymd)

        daily_path_use = pb_io.path_replace_ymd(daily_path, ymd)
        com_out_file = os.path.join(daily_path_use, com_filename)

        granule_path_use = pb_io.path_replace_ymd(granule_path, ymd)
        reg = 'FY3[A-Z]_MERSI_ORBT_L2_ASO_MLT_NUL_%s_(\d{4})_1000M.HDF' % ymd
        granule_lst = find_file(granule_path_use, reg)

        reg = 'FY3[A-Z]_MERSI_ORBT_L2_ASO_MLT_NUL_%s_(\d{4})_5000M.HDF' % ymd
        proj_path_use = pb_io.path_replace_ymd(proj_path, ymd)
        granule_pro_lst = find_file(proj_path_use, reg)
        print len(granule_pro_lst)

        com_dict = {
            'PATH': {'ipath': granule_lst, 'ppath': granule_pro_lst, 'opath': com_out_file}}
        cfgFile = os.path.join(cfg_path, '%s.yaml' % ymd)
        CreateYamlCfg(com_dict, cfgFile)

        date_s = date_s + relativedelta(days=1)

    reg = '.*.yaml'
    FileLst = find_file(cfg_path, reg)
    FileLst = [each + '\n' for each in FileLst]
    fp = open('filelist.txt', 'w')
    fp.writelines(FileLst)
    fp.close()


def create_06_combine_d_map(date_s, date_e):

    daily_path = inCfg['PATH']['OUT']['daily']
    FileLst = []
    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        timeStep = relativedelta(days=1)
        daily_path_use = pb_io.path_replace_ymd(daily_path, ymd)
        com_filename = '%s_%s_GBAL_L3_ASO_MLT_GLL_%s_AOAD_5000M.HDF' % (
            inCfg['PATH']['sat'], inCfg['PATH']['sensor'], ymd)
        FileLst.append(os.path.join(daily_path_use, com_filename))
        date_s = date_s + timeStep

    FileLst = [each + '\n' for each in FileLst]
    fp = open('filelist.txt', 'w')
    fp.writelines(FileLst)
    fp.close()


def CreateYamlCfg(yaml_dict, cfgFile):
    # 投影程序需要的配置文件名称
    cfgPath = os.path.dirname(cfgFile)
    if not os.path.isdir(cfgPath):
        os.makedirs(cfgPath)
    with open(cfgFile, 'w') as stream:
        yaml.dump(yaml_dict, stream, default_flow_style=False)


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


def run_parallel(exe, npnums, cfg=None):

    mpiRun = '/usr/mpi/intel/mvapich-1.2.0/bin/mpirun_rsh'
    if cfg is None:
        cmd = '%s -np %s -hostfile hostfile parallel.exe -l \
        ./filelist.txt -e "%s"' % (mpiRun, npnums, exe)
    else:
        cmd = '%s -np %s -hostfile hostfile parallel.exe -l \
        ./filelist.txt -e %s -c %s' % (mpiRun, npnums, exe, cfg)
    os.system(cmd)


def find_file(path, reg):
    '''
    path: 要遍历的目录
    reg: 符合条件的文件
    '''
    FileLst = []
    try:
        lst = os.walk(path)
        for root, dirs, files in lst:
            for name in files:
                try:
                    m = re.match(reg, name)
                except Exception as e:
                    continue
                if m:
                    FileLst.append(os.path.join(root, name))
    except Exception as e:
        print str(e)

    return sorted(FileLst)

if __name__ == '__main__':

    main()

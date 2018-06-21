# coding=utf-8

from datetime import datetime
from multiprocessing import Pool, Lock
from time import ctime
import getopt
import os
import re
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
__updated__ = '2018-06-21'

python = 'python27 -W ignore'

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

    in_path = inCfg['PATH']['IN']['l1']
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


def job_0211(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :数据压缩
    '''
    Log.info(u'get arg list from job_0211')
    inpath = inCfg['PATH']['IN']['ncep']
    arg_list = []
    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        use_inpath = pb_io.path_replace_ymd(inpath, ymd)
        file_list = pb_io.find_file(use_inpath, reg)
        for in_file in file_list:
            cmd_list = '%s %s %s %s' % (python, job_exe, sat_pair, in_file)
            arg_list.append(cmd_list)
        date_s = date_s + relativedelta(days=1)
    return arg_list


def job_0310(job_exe, sat_pair, date_s, date_e, job_id):
    Log.info(u'get arg list from job_0310')
    return job_0110(job_exe, sat_pair, date_s, date_e, job_id)


def job_0410(job_exe, sat_pair, date_s, date_e, job_id):
    Log.info(u'get arg list from job_0410')
    # 精匹配
    craete_incfg_job_04(sat_pair, date_s, date_e, job_id)
    arg_list = get_arglist_job01_04(job_exe,
                                    sat_pair, date_s, date_e, job_id)
    return arg_list


def job_0510(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :日合成
    '''
    Log.info(u'get arg list from job_0510')
    arg_list = []
    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')

        cmd_list = '%s %s %s %s-%s' % (python, job_exe, sat_pair, ymd, ymd)
        arg_list.append(cmd_list)
        date_s = date_s + relativedelta(days=1)
    return arg_list


def job_0511(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :日合成
    '''
    Log.info(u'get arg list from job_0511')
    return job_0510(job_exe, sat_pair, date_s, date_e, job_id)


def job_0610(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :日合成
    '''
    Log.info(u'get arg list from job_0610')
    arg_list = []
    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')

        cmd_list = '%s %s %s %s-%s' % (python, job_exe, sat_pair, ymd, ymd)
        arg_list.append(cmd_list)
        date_s = date_s + relativedelta(days=1)
    return arg_list


def job_0710(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :国际标准nc生成
    '''
    Log.info(u'get arg list from job_0710')
    arg_list = []
    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')

        cmd_list = '%s %s %s %s' % (python, job_exe, sat_pair, ymd)
        arg_list.append(cmd_list)
        date_s = date_s + relativedelta(days=1)
    return arg_list


def job_0810(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :国际标准nc合成
    '''
    Log.info(u'get arg list from job_0810')
    arg_list = []

    cmd_list = '%s %s %s ' % (python, job_exe, sat_pair)
    arg_list.append(cmd_list)
    return arg_list


def job_0910(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :蝴蝶图和日回归
    '''
    Log.info(u'get arg list from job_0910')
    arg_list = []
    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')

        cmd_list = '%s %s %s %s' % (python, job_exe, sat_pair, ymd)
        arg_list.append(cmd_list)
        date_s = date_s + relativedelta(days=1)
    return arg_list


def job_1010(job_exe, sat_pair, date_s, date_e, job_id):
    Log.info(u'get arg list from job_1010')
    return job_0910(job_exe, sat_pair, date_s, date_e, job_id)


def job_1110(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :月回归
    '''
    Log.info(u'get arg list from job_1110')
    arg_list = []
    while date_s <= date_e:
        ym = date_s.strftime('%Y%m')

        cmd_list = '%s %s %s %s' % (python, job_exe, sat_pair, ym)
        arg_list.append(cmd_list)
        date_s = date_s + relativedelta(months=1)
    return arg_list


def job_1210(job_exe, sat_pair, date_s, date_e, job_id):
    Log.info(u'get arg list from job_1210')
    return job_1110(job_exe, sat_pair, date_s, date_e, job_id)


def job_1310(job_exe, sat_pair, date_s, date_e, job_id):
    '''
    :长时间序列图
    '''
    Log.info(u'get arg list from job_1310')
    arg_list = []

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        ymd_year_befor = (date_s - relativedelta(years=1)).strftime('%Y%m%d')
        cmd_list = '%s %s %s %s-%s' % (python,
                                       job_exe, sat_pair, ymd_year_befor, ymd)
        arg_list.append(cmd_list)
        date_s = date_s + relativedelta(days=1)
    cmd_list = '%s %s %s' % (python, job_exe, sat_pair)
    arg_list.append(cmd_list)
    return arg_list


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


def craete_incfg_job_01_03(sat_pair, date_s, date_e, job_id):

    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        # 固定点
        if 'FIX' in sat_pair:
            create_leo_fix(sat_pair, ymd, job_id)
        elif 'AREA' in sat_pair:
            create_leo_area(sat_pair, ymd, job_id)
        # cross
        else:
            create_leo_leo_cross(sat_pair, ymd, job_id)

        date_s = date_s + relativedelta(days=1)


def craete_incfg_job_04(sat_pair, date_s, date_e, job_id):
    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')

        create_leo_leo_fine(sat_pair, ymd, job_id)

        date_s = date_s + relativedelta(days=1)


def create_leo_area(sat_pair, ymd, job_id):

    shortsat1 = (sat_pair.split('_')[0]).split('+')[0]

    # 解析 cross.cfg中的信息
    NUM1 = inCfg['PAIRS'][sat_pair]['num1']
    NUM2 = inCfg['PAIRS'][sat_pair]['num2']

    # 根据编号在dm_odm.cfg中查找到对应的数据描述信息
    sat1 = odmCfg['ORDER'][NUM1[0]]['SELF_SAT']
    sensor1 = odmCfg['ORDER'][NUM1[0]]['SELF_SENSOR']
    product1 = odmCfg['ORDER'][NUM1[0]]['SELF_PRODUCT']
    interval1 = odmCfg['ORDER'][NUM1[0]]['SELF_INTERVAL']
    reg1 = odmCfg['ORDER'][NUM1[0]]['SELF_REG'][0]

    # 存放俩颗卫星的原始数据目录位置
    inpath1 = os.path.join(DATA_DIR, '%s/%s/%s/%s' %
                           (sat1, sensor1, product1, interval1), ymd[:6])

    # 根据num1对应的数据找到迁移数据清单，提取数据列表
    file_list = find_file(inpath1, ymd, reg1)

    # 将所有交叉点时间，根据sec1偏差变为时间段
    timeList = []
    # 拼接预报文件,读取固定点交叉预报文件进行文件获取
    for area_name in NUM2:
        ForecastFile = os.path.join(
            CROSS_DIR, sat1 + '_' + area_name, sat1 + '_' + area_name + '_' + ymd + '.txt')

        fp = open(ForecastFile, 'r')
        Lines = fp.readlines()
        fp.close()

        for Line in Lines[10:]:
            s_hms = Line.split()[1].strip()
            e_hms = Line.split()[2].strip()
            s_lon = float(Line.split()[4].strip())
            e_lon = float(Line.split()[6].strip())
            cLon = (s_lon + e_lon) / 2.  # 判断此时的地区是否是白天
            if not pb_time.lonIsday(ymd, s_hms, cLon):
                #  u'晚上过滤掉'
                continue
            cross_time1 = datetime.strptime(
                '%s %s' % (ymd, s_hms), '%Y%m%d %H:%M:%S')
            cross_time2 = datetime.strptime(
                '%s %s' % (ymd, e_hms), '%Y%m%d %H:%M:%S')
            timeList.append([cross_time1, cross_time2])
        c_time_list = pb_time.CombineTimeList(timeList)

        file_list1 = []
        for i in xrange(len(c_time_list)):
            t_file_list = Find_data_FromCrossTime(
                file_list, c_time_list[i][0], c_time_list[i][1])

            file_list1.extend(t_file_list)

        # 区域名称
        area_lon_lat_range = odmCfg['AREA_LIST'][area_name]
        left_lat1, right_lat2, left_lon1, right_lon2 = area_lon_lat_range
        left_lon1 = float(left_lon1)
        left_lat1 = float(left_lat1)
        right_lon2 = float(right_lon2)
        right_lat2 = float(right_lat2)

        print left_lon1, left_lat1, right_lon2, right_lat2
        center_lon = left_lon1 + (right_lon2 - left_lon1) / 2.
        center_lat = left_lat1 + (right_lat2 - left_lat1) / 2.
        print center_lon, center_lat
        # 投影后结果的命名方式
        projName1 = '%s_%s_GBAL_L1_%s_%s_proj_lon%+08.3f_lat%+08.3f.hdf' % (
            shortsat1, sensor1, ymd, area_name, center_lon, center_lat)
        # 投影后的输出全路径名称
        projName1 = os.path.join(
            PROJ_DIR, sat_pair, area_name, ymd[:6], projName1)

        cmd = '+proj=laea  +lat_0=%f +lon_0=%f +x_0=0 +y_0=0 +ellps=WGS84' % (
            center_lat, center_lon)

        cfgFile1 = os.path.join(
            JOBCFG_DIR, sat_pair, job_id, ymd, '%s_%s.yaml' % (ymd, area_name))

        # 调用创建投影配置文件程序生成配置，其中南极和北极需要的分辨率是 512*512，其他256*256
        if len(file_list1) != 0:
            Log.info('%s %s create success' % (ymd, area_name))
            res = 1000
            deg = res / 100000.
            col = int((right_lon2 - left_lon1) / deg)
            row = int((right_lat2 - left_lat1) / deg)

            print row, col
            dict1 = {'INFO': {'sat': shortsat1, 'sensor': sensor1, 'ymd': ymd},
                     'PROJ': {'cmd': cmd, 'row': row, 'col': col, 'res': res},
                     'PATH': {'opath': projName1, 'ipath': file_list1}}
            CreateYamlCfg(dict1, cfgFile1)


def create_leo_fix(sat_pair, ymd, job_id):
    '''
    创建 极轨卫星 和 固定点 的作业配置文件
    '''
    shortsat1 = (sat_pair.split('_')[0]).split('+')[0]

    # 解析 cross.cfg中的信息
    NUM1 = inCfg['PAIRS'][sat_pair]['num1']
    NUM2 = inCfg['PAIRS'][sat_pair]['num2']
    sec1 = inCfg['PAIRS'][sat_pair]['sec1']

    # 根据编号在dm_odm.cfg中查找到对应的数据描述信息
    sat1 = odmCfg['ORDER'][NUM1[0]]['SELF_SAT']
    sensor1 = odmCfg['ORDER'][NUM1[0]]['SELF_SENSOR']
    product1 = odmCfg['ORDER'][NUM1[0]]['SELF_PRODUCT']
    interval1 = odmCfg['ORDER'][NUM1[0]]['SELF_INTERVAL']
    reg1 = odmCfg['ORDER'][NUM1[0]]['SELF_REG'][0]

    # 存放俩颗卫星的原始数据目录位置
    inpath1 = os.path.join(DATA_DIR, '%s/%s/%s/%s' %
                           (sat1, sensor1, product1, interval1), ymd[:6])

    # 将所有交叉点时间，根据sec1偏差变为时间段
    timeList = []
    # 拼接预报文件,读取固定点交叉预报文件进行文件获取
    ForecastFile = os.path.join(
        CROSS_DIR, sat1 + '_' + 'FIX', sat1 + '_' + 'FIX' + '_' + ymd + '.txt')

    if os.path.isfile(ForecastFile):
        fp = open(ForecastFile, 'r')
        Lines = fp.readlines()
        fp.close()

        for Line in Lines[10:]:
            hms = Line.split()[1].strip()
            name = Line.split()[2].strip()
            lat = float(Line.split()[3].strip())
            lon = float(Line.split()[4].strip())
            cross_time = datetime.strptime(
                '%s %s' % (ymd, hms), '%Y%m%d %H:%M:%S')
            timeList.append([cross_time, name, lat, lon])

    if len(timeList) <= 0:
        Log.error('cross nums: 0')
        return

    # 每个固定点的组时间偏差sec是不一样的，在这里做区分标记
    cross_sec_group = {}
    for i in xrange(len(NUM2)):
        cross_sec_group[NUM2[i]] = sec1[i]

    # 根据num1对应的数据找到迁移数据清单，提取数据列表
    file_list = find_file(inpath1, ymd, reg1)

    # 根据每个交叉点进行判断，并创建投影的配置文件
    for crossTime in timeList:
        # 根据站点分组，找到对应每个组下面的具体站点名称
        for group in NUM2:
            fixList = odmCfg['FIX_LIST'][group]
            # timeList 0=交叉点时间    1=站点名称  2=经度  3=纬度
            if crossTime[1] in fixList:
                # 根据分组，提取站点对应偏差的秒数，然后把交叉点时间变为时间段
                secs = cross_sec_group[group]
                s_cross_time1 = crossTime[0] - relativedelta(seconds=int(secs))
                e_cross_time1 = crossTime[0] + relativedelta(seconds=int(secs))
                # 从数据列表中查找过此交叉点时间的数据块,查到过此固定点的数据 select File
                file_list1 = Find_data_FromCrossTime(
                    file_list, s_cross_time1, e_cross_time1)
                # 交叉点时间，固定点名称，纬度，经度
                ymdhms = crossTime[0].strftime('%Y%m%d%H%M%S')
                ymdhms1 = crossTime[0].strftime('%Y%m%d %H:%M:%S')
                fixName = crossTime[1]
                Lat = crossTime[2]
                Lon = crossTime[3]

                # 投影后结果的命名方式
                projName1 = '%s_%s_GBAL_L1_%s_%s_proj_lon%+08.3f_lat%+08.3f.hdf' % (
                    shortsat1, sensor1, ymdhms[:8], ymdhms[8:12], Lon, Lat)
                # 投影后的输出全路径名称
                projName1 = os.path.join(
                    PROJ_DIR, sat_pair, ymdhms[:6], projName1)

                cmd = '+proj=laea  +lat_0=%f +lon_0=%f +x_0=0 +y_0=0 +ellps=WGS84' % (
                    Lat, Lon)

                cfgFile1 = os.path.join(JOBCFG_DIR, sat_pair, job_id, ymdhms[
                                        :8], '%s_%s.yaml' % (ymdhms, fixName))

                # 调用创建投影配置文件程序生成配置，其中南极和北极需要的分辨率是 512*512，其他256*256
                if len(file_list1) != 0:
                    Log.info('%s %s create success' % (ymdhms1, fixName))
                    if 'Dome_C' in fixName or 'Greenland' in fixName or 'xinjiangaletai' in fixName:
                        dict1 = {'INFO': {'sat': shortsat1, 'sensor': sensor1, 'ymd': ymdhms},
                                 'PROJ': {'cmd': cmd, 'row': 512, 'col': 512, 'res': 1000},
                                 'PATH': {'opath': projName1, 'ipath': file_list1}}
                        CreateYamlCfg(dict1, cfgFile1)
                    else:
                        dict1 = {'INFO': {'sat': shortsat1, 'sensor': sensor1, 'ymd': ymdhms},
                                 'PROJ': {'cmd': cmd, 'row': 256, 'col': 256, 'res': 1000},
                                 'PATH': {'opath': projName1, 'ipath': file_list1}}
                        CreateYamlCfg(dict1, cfgFile1)
                else:
                    Log.error('%s %s create failed ' % (ymdhms1, fixName))


def create_leo_leo_fine(sat_pair, ymd, job_id):
    '''
    创建精匹配配置接口文件
    '''
    # 解析mathcing: FY3A+MERSI_AQUA+MODIS ,根据下划线分割获取 卫星+传感器 ,再次分割获取俩颗卫星短名
    shortsat1 = (sat_pair.split('_')[0]).split('+')[0]
    shortsat2 = (sat_pair.split('_')[1]).split('+')[0]
    # 解析global.cfg中的信息
    NUM1 = inCfg['PAIRS'][sat_pair]['num1']
    NUM2 = inCfg['PAIRS'][sat_pair]['num2']
    sec1 = int(inCfg['PAIRS'][sat_pair]['sec1'])
#     sec2 = inCfg['PAIRS'][sat_pair]['sec2']

    # 根据编号在dm_odm.cfg中查找到对应的数据描述信息
    sat1 = odmCfg['ORDER'][NUM1[0]]['SELF_SAT']
    sensor1 = odmCfg['ORDER'][NUM1[0]]['SELF_SENSOR']
    product1 = odmCfg['ORDER'][NUM1[0]]['SELF_PRODUCT']
    interval1 = odmCfg['ORDER'][NUM1[0]]['SELF_INTERVAL']
    reg1 = odmCfg['ORDER'][NUM1[0]]['SELF_REG'][0]

    sat2 = odmCfg['ORDER'][NUM2[0]]['SELF_SAT']
    sensor2 = odmCfg['ORDER'][NUM2[0]]['SELF_SENSOR']
    product2 = odmCfg['ORDER'][NUM2[0]]['SELF_PRODUCT']
    interval2 = odmCfg['ORDER'][NUM2[0]]['SELF_INTERVAL']
    reg2 = odmCfg['ORDER'][NUM2[0]]['SELF_REG'][0]

    # 存放俩颗卫星的原始数据目录位置
    inpath1 = os.path.join(DATA_DIR, '%s/%s/%s/%s' %
                           (sat1, sensor1, product1, interval1), ymd[:6])
    inpath2 = os.path.join(DATA_DIR, '%s/%s/%s/%s' %
                           (sat2, sensor2, product2, interval2), ymd[:6])

    file_list1 = find_file(inpath1, ymd, reg1)
    file_list2 = find_file(inpath2, ymd, reg2)

    # file_list2是高光普数据，根据list2找list1符合条件的数据
    for filename2 in file_list2:
        name2 = os.path.basename(filename2)
        nameClass = pb_name.nameClassManager()
        info = nameClass.getInstance(name2)
        if info is None:
            continue
        # 获取数据时间段,用开始时间进行加减 05 10 15
        data_stime2 = info.dt_s - relativedelta(seconds=sec1)
        data_etime2 = info.dt_s + relativedelta(seconds=sec1)
#         print data_stime2, data_etime2

        ymdhms = info.dt_s.strftime('%Y%m%d%H%M%S')

        new_file_list1 = Find_data_FromCrossTime(
            file_list1, data_stime2, data_etime2)

        yaml_file = os.path.join(
            JOBCFG_DIR, sat_pair, job_id, ymdhms[:8], '%s_%s_%s.yaml' % (ymdhms, sensor1, sensor2))

        # 输出文件命名
        filename = 'W_CN-CMA-NSMC,SATCAL+NRTC,GSICS+MATCHEDPOINTS,%s_C_BABJ_%s.hdf5' % (
            sat_pair, ymdhms)

        # 输出完整路径
        full_filename = os.path.join(
            MATCH_DIR, sat_pair, ymdhms[:4], ymdhms[:8], filename)
        if len(new_file_list1) > 0:
            dict1 = {'INFO': {'sat1': shortsat1, 'sensor1': sensor1, 'sat2': shortsat2, 'sensor2': sensor2, 'ymd': ymdhms},
                     'PATH': {'opath': full_filename, 'ipath1': new_file_list1, 'ipath2': [filename2]}}
            CreateYamlCfg(dict1, yaml_file)


def create_leo_leo_cross(sat_pair, ymd, job_id):
    '''
    创建 极轨卫星 和 极轨卫星 的作业配置文件
    '''
    # 解析mathcing: FY3A+MERSI_AQUA+MODIS ,根据下划线分割获取 卫星+传感器 ,再次分割获取俩颗卫星短名
    shortsat1 = (sat_pair.split('_')[0]).split('+')[0]
    shortsat2 = (sat_pair.split('_')[1]).split('+')[0]
    # 解析global.cfg中的信息
    NUM1 = inCfg['PAIRS'][sat_pair]['num1']
    NUM2 = inCfg['PAIRS'][sat_pair]['num2']
    sec1 = inCfg['PAIRS'][sat_pair]['sec1']
    sec2 = inCfg['PAIRS'][sat_pair]['sec2']

    # 根据编号在dm_odm.cfg中查找到对应的数据描述信息
    sat1 = odmCfg['ORDER'][NUM1[0]]['SELF_SAT']
    sensor1 = odmCfg['ORDER'][NUM1[0]]['SELF_SENSOR']
    product1 = odmCfg['ORDER'][NUM1[0]]['SELF_PRODUCT']
    interval1 = odmCfg['ORDER'][NUM1[0]]['SELF_INTERVAL']
    reg1 = odmCfg['ORDER'][NUM1[0]]['SELF_REG'][0]

    sat2 = odmCfg['ORDER'][NUM2[0]]['SELF_SAT']
    sensor2 = odmCfg['ORDER'][NUM2[0]]['SELF_SENSOR']
    product2 = odmCfg['ORDER'][NUM2[0]]['SELF_PRODUCT']
    interval2 = odmCfg['ORDER'][NUM2[0]]['SELF_INTERVAL']
    reg2 = odmCfg['ORDER'][NUM2[0]]['SELF_REG'][0]

    # 存放俩颗卫星的原始数据目录位置
    inpath1 = os.path.join(DATA_DIR, '%s/%s/%s/%s' %
                           (sat1, sensor1, product1, interval1), ymd[:6])
    inpath2 = os.path.join(DATA_DIR, '%s/%s/%s/%s' %
                           (sat2, sensor2, product2, interval2), ymd[:6])

    # 读取交叉点上的俩颗卫星的交叉时间，1列=经度  2列=纬度  3列=卫星1时间  4列=卫星2时间
    timeList = ReadCrossFile_LEO_LEO(sat1, sat2, ymd)
    if len(timeList) <= 0:
        Log.error('cross nums: 0')
        return

    file_list1 = find_file(inpath1, ymd, reg1)
    file_list2 = find_file(inpath2, ymd, reg2)

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

        # 存放匹配信息的yaml配置文件存放位置
        yaml_file1 = os.path.join(
            JOBCFG_DIR, sat_pair, job_id, ymdhms[:8], '%s_%s.yaml' % (ymdhms, sensor1))

        yaml_file2 = os.path.join(
            JOBCFG_DIR, sat_pair, job_id, ymdhms[:8], '%s_%s.yaml' % (ymdhms, sensor2))

        yaml_file3 = os.path.join(
            JOBCFG_DIR, sat_pair, job_id, ymdhms[:8], '%s_%s_%s.yaml' % (ymdhms, sensor1, sensor2))

        # 输出文件命名
        filename1 = '%s_%s_GBAL_L1_%s_%s_proj_lon%+08.3f_lat%+08.3f.hdf' % (
            shortsat1, sensor1, ymdhms[:8], ymdhms[8:12], Lon, Lat)
        filename2 = '%s_%s_GBAL_L1_%s_%s_proj_lon%+08.3f_lat%+08.3f.hdf' % (
            shortsat2, sensor2, ymdhms[:8], ymdhms[8:12], Lon, Lat)
        filename3 = 'W_CN-CMA-NSMC,SATCAL+NRTC,GSICS+MATCHEDPOINTS,%s_C_BABJ_%s.hdf5' % (
            sat_pair, ymdhms)

        # 输出完整路径
        full_filename1 = os.path.join(
            PROJ_DIR, sat_pair, ymdhms[:6], filename1)
        full_filename2 = os.path.join(
            PROJ_DIR, sat_pair, ymdhms[:6], filename2)
        full_filename3 = os.path.join(
            MATCH_DIR, sat_pair, ymdhms[:6], filename3)

        # 投影参数
        cmd = '+proj=laea  +lat_0=%f +lon_0=%f +x_0=0 +y_0=0 +ellps=WGS84' % (
            Lat, Lon)

        if len(list1) > 0:

            dict1 = {'INFO': {'sat': shortsat1, 'sensor': sensor1, 'ymd': ymdhms},
                     'PROJ': {'cmd': cmd, 'row': 1024, 'col': 1024, 'res': 1000},
                     'PATH': {'opath': full_filename1, 'ipath': list1}}

            if '0110' in job_id:
                Log.info('%s %s %s create proj1 cfg success' %
                         (shortsat1, sensor1, ymdhms))
                CreateYamlCfg(dict1, yaml_file1)

        if len(list2) > 0:

            dict2 = {'INFO': {'sat': shortsat2, 'sensor': sensor2, 'ymd': ymdhms},
                     'PROJ': {'cmd': cmd, 'row': 1024, 'col': 1024, 'res': 1000},
                     'PATH': {'opath': full_filename2, 'ipath': list2}}

            if '0210' in job_id:
                Log.info('%s %s %s create proj1 cfg success' %
                         (shortsat2, sensor2, ymdhms))
                CreateYamlCfg(dict2, yaml_file2)

        if len(list1) > 0 and len(list2) > 0:

            row = 1024
            col = 1024
            res = 1000

            if sensor1 in ['MERSI', 'VIRR'] and sensor2 in ['MODIS', 'VIIRS']:
                row = 128
                col = 128
                res = 8000
            elif sensor1 in ['MERSI', 'VIRR'] in ['IASI', 'GOME', 'CRIS']:
                row = 4100
                col = 4100
                res = 1000
            elif sensor1 in ['IRAS'] and sensor2 in ['IASI', 'CRIS']:
                row = 512
                col = 512
                res = 15000
#                 row = 1054
#                 col = 2113
#                 res = 17000
#                 half_res = res / 2.
#                 cmd = '+proj=eqc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=-%f +y_0=%f +datum=WGS84' % (
#                     half_res, half_res)

            dict3 = {'INFO': {'sat1': shortsat1, 'sensor1': sensor1, 'sat2': shortsat2, 'sensor2': sensor2, 'ymd': ymdhms},
                     'PATH': {'opath': full_filename3, 'ipath1': list1, 'ipath2': list2},
                     'PROJ': {'cmd': cmd, 'row': row, 'col': col, 'res': res}}

            if '0310' in job_id:
                Log.info('%s %s create collocation cfg success' %
                         (sat_pair, ymdhms))
                CreateYamlCfg(dict3, yaml_file3)


def ReadCrossFile_LEO_LEO(sat1, sat2, ymd):

    # 本模块于2017-12-14添加了snox的订购。订购时应注意相同卫星对的情况下，cross与snox内卫星前后顺序是否一致。
    #     timeList = []
    # 拼接cross, snox预报文件
    Filedir = sat1 + '_' + sat2
    FileName1 = Filedir + '_' + ymd + '.txt'
    FileName2 = Filedir + '_' + 'SNOX' + '_' + ymd + '.txt'
    crossFile = os.path.join(CROSS_DIR, Filedir, FileName1)
    snoxFile = os.path.join(SNOX_DIR, Filedir, FileName2)
    index1 = (1, 2, 3, 4)
    index2 = (1, 2, 3, 4)
    if not os.path.isfile(crossFile):  # 不存在则调换卫星顺序
        Filedir = sat2 + '_' + sat1
        FileName = sat2 + '_' + sat1 + '_' + ymd + '.txt'
        crossFile = os.path.join(CROSS_DIR, Filedir, FileName)
        index1 = (4, 5, 6, 1)

    if not os.path.isfile(snoxFile):  # 不存在则调换卫星顺序
        Filedir = sat2 + '_' + sat1
        FileName = Filedir + '_' + 'SNOX' + '_' + ymd + '.txt'
        snoxFile = os.path.join(SNOX_DIR, Filedir, FileName)
        index2 = (4, 5, 6, 1)

    Lines1 = []
    Lines2 = []
    # 交叉点预报文件内容
    if os.path.isfile(crossFile):
        fp = open(crossFile, 'r')
        bufs = fp.readlines()
        fp.close()
        # 获取长度不包含头信息
        Lines1 = bufs[10:]

    # 近重合预报文件内容
    if os.path.isfile(snoxFile):
        fp = open(snoxFile, 'r')
        bufs = fp.readlines()
        fp.close()
        # 获取长度
        Lines2 = bufs[10:]

    timelst1 = get_cross_file_timelist(Lines1, index1)
    timelst2 = get_cross_file_timelist(Lines2, index2)
    timeList = timelst1 + timelst2

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

    if s_ymdhms2 <= s_ymdhms1 <= e_ymdhms2:
        return True
    elif s_ymdhms2 < e_ymdhms1 <= e_ymdhms2:
        return True
    elif s_ymdhms2 > s_ymdhms1 and e_ymdhms2 < e_ymdhms1:
        return True
    else:
        return False


def find_file(path, ymd, reg):
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
                    print str(e)
                    continue
                if m:
                    nameClass = pb_name.nameClassManager()
                    info = nameClass.getInstance(name)
                    if info is None:
                        continue
                    if info.dt_s.strftime('%Y%m%d') != ymd:
                        continue
                    FileLst.append(os.path.join(root, name))
    except Exception as e:
        print str(e)

    return sorted(FileLst)

if __name__ == '__main__':

    main()

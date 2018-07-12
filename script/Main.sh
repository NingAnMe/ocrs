#!/bin/bash
#===============================================================
# Name:         Main.sh
# author:       wangpeng
# Type:         Bash Shell Script
# Description:  项目总体调度脚本
#===============================================================

#配置环境变量
ROOT_DIR=/aerosol
export CODA_DEFINITION=$ROOT_DIR/Project/PB/DRC/definitions
export PYTHONPATH=$ROOT_DIR/Project
export OMP_NUM_THREADS=4
export PATH=$ROOT_DIR/anaconda2/bin:/usr/local/bin:$PATH

# dirname $0 是执行次脚本的位置, cd 后在获取位置才是绝对位置basepath,所以要cd basepath
basepath=$(cd `dirname $0`; pwd)
cd $basepath
bashName=`basename $0`
logName=`echo $bashName |awk -F '.' '{print $1}'`.log
echo `date +"%Y-%m-%d %H:%M:%S"` 'star gsics global crond...' >> $logName

# 项目位置
proj_path=${basepath%/*}

# 1.1 开始同步报文等文件
./download_ncep.sh

# 1.2 FY3B/D 软链接开始创建

./create_link_fy3d.sh


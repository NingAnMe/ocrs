#!/bin/bash
stime=$1
etime=$2
#########创建多线程#####################
tmp_fifofile="/tmp/$$.fifo"
mkfifo  $tmp_fifofile            #  新建一个fifo类型的文件
exec 6<> $tmp_fifofile           #  将fd6指向fifo类型
rm  $tmp_fifofile
thread=20 #  此处定义线程数
for  ((t=0 ;t<$thread ;t++ ));
do
    echo
done >&6
while :
do
    if [[ $stime -gt $etime ]];then
        break
    fi
    read -u6
    {
        python27 ocrs_fy3_mersi_calibration.py $stime-$stime
        echo >&6
    }&
    stime=$(date -d "$stime 1day"  +%Y%m%d)
done
wait
exec 6>&-
exit 0

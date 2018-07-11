#!/bin/bash

if [ $# -eq 0 ];then
    stime=`date +"%Y%m%d" -d "5 days ago"`
    etime=`date +"%Y%m%d" -d "1 days ago"`
else
    stime=$1
    etime=$2
fi

time_list="00 06 12 18"


while :
do
    ncep_dir=/aerosol/CMA_AEROSOL/IDATA/NcepData/${stime:0:4}/${stime:0:6}
    if [ ! -d $ncep_dir ];then
        mkdir -p $ncep_dir
    fi
    for time in $time_list
    do
        ftpadd="ftp://10.20.49.124//cma/g2/COMMDATA/glob/fnl/"${stime:0:4}"/fnl_"$stime"_"$time"_00.grib2"
        wget --ftp-user=minmin --ftp-password=passw0rd  --no-parent \
    	-nd -nH -c  --dont-remove-listing $ftpadd -O ${ncep_dir}/fnl_"$stime"_"$time"_00_c
    done
    stime=$(date -d "$stime 1day"  +%Y%m%d)
    if [[ $stime -gt $etime ]]; then
        break;
    fi
done


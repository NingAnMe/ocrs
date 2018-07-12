#!/bin/bash
# 创建 FY3D 的MERSI和HIRAS数据归档目录软连接
# 20180430

function createLink()
{
    srcPath=$1
    desPath=$2
    #mvPath=$3
    if [ ! -d $desPath ];then
        mkdir -p $desPath
    fi 
    for infile in `find $srcPath -type f -name *.HDF | sort`
    do
        name=${infile##*/}
        #echo $name >> $mvPath
        desFile=$desPath/$name
        if [ ! -f $desFile ];then
            ln -s $infile $desFile
        fi
        
    done
}

if [ $# -eq 0 ];then
    stime=`date +"%Y%m%d" -d "5 days ago"`
    etime=`date +"%Y%m%d"` 
else
    stime=$1
    etime=$2
fi
# 进入目录
basepath=$(cd `dirname $0`; pwd)
cd $basepath
bashName=`basename $0`

pnums=`ps x |grep -w $bashName |grep -v grep | wc -l`
cfgName=`echo $bashName |awk -F '.' '{print $1}'`.cfg
logName=`echo $bashName |awk -F '.' '{print $1}'`.log

if [ $pnums -ge 3 ]; then
    echo `date +%Y%m%d` "too many processes $pnums are running, so exit." >> $logName
    exit
fi

echo `date +"%Y-%m-%d %H:%M:%S"` 'star create link...' >> $logName

while :
do
    while read line
    do
        year=${stime:0:4}
        mon=${stime:4:2}
        day=${stime:6:2}
        path1=`echo $line | awk '{print $1}'`
        path2=`echo $line | awk '{print $2}'`
        #path3=`echo $line | awk '{print $3}'`
        
        if [ ! -d $path3 ];then
            mkdir -p $path3
        fi
        
        newpath1=`echo $path1 |sed "s#YYYY#${year}#g" |sed "s#MM#${mon}#g" |sed "s#DD#${day}#g"`
        newpath2=`echo $path2 |sed "s#YYYY#${year}#g" |sed "s#MM#${mon}#g" |sed "s#DD#${day}#g"`
        if [ -d $newpath1 ];then
            echo $newpath1 $newpath2
            #createLink $newpath1 $newpath2  $path3/${stime}.txt
            createLink $newpath1 $newpath2
        else
            continue
        fi
    done < $cfgName

    stime=$(date -d "$stime 1day"  +%Y%m%d)
    if [[ $stime -gt $etime ]]; then
        break;
    fi

done



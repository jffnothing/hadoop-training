#!/bin/bash
a=`date +%Y%m%d`
mkdir $a
tar -zcf /home/zkpk/$a/hadooplog.tar.gz /home/zkpk/hadoop/logs/*
scp -r /home/zkpk/$a zkpk@slave:/home/zkpk/logs/master



.................................
退出该脚本后执行以下代码：
crontab -e
0 2 * * 5 /home/zkpk/8-13.bash

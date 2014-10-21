#!/bin/bash
#yum install mysql
#yum install mysql-server
chkconfig --add mysqld
chkconfig --level 35 mysqld on
service mysqld start
#mysqladmin -u root password "123456"
mysql -u root -p123456 <<EOF
#show databases;
#create database mytable;
use mytable;
#create table mytable(time varchar(30),uid varchar(30),keyword varchar(30),rank varchar(30),ord varchar(30),url varchar(30));
#load data local infile "/home/zkpk/sogou" into table mytable;
#flush privileges;
select uid,count(*) as cishu from mytable 
group by uid
order by cishu desc limit 0,10 into outfile "/home/zkpk/result"; 
EOF

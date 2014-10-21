#!/bin/bash
hadoop jar /home/zkpk/hbase/hbase-0.94.21.jar importtsv 
-Dimporttsv.bulk.output=/user/hbase/output 
-Dimporttsv.columns=HBASE_ROW_KEY,s1:time,s1:keyword,s1:rank,s1:order,s1:url 
sogou.100w.tsv /user/hbase/input/sogou.100w.uid

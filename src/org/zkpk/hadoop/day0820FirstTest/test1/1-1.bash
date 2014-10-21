#!/bin/bash
cat /home/zkpk/1-5.txt | awk -F "," '{print $1"\t"$2"\t""2014/1/1\t"$7"\t"$8"\t"$9}' > /home/zkpk/test1/1_1.txt
cat /home/zkpk/1-5.txt | awk -F "," '{print $1"\t"$3"\t""2014/1/2\t"$7"\t"$8"\t"$9}' > /home/zkpk/test1/1_2.txt
cat /home/zkpk/1-5.txt | awk -F "," '{print $1"\t"$4"\t""2014/1/3\t"$7"\t"$8"\t"$9}' > /home/zkpk/test1/1_3.txt
cat /home/zkpk/1-5.txt | awk -F "," '{print $1"\t"$5"\t""2014/1/4\t"$7"\t"$8"\t"$9}' > /home/zkpk/test1/1_4.txt
cat /home/zkpk/1-5.txt | awk -F "," '{print $1"\t"$6"\t""2014/1/5\t"$7"\t"$8"\t"$9}' > /home/zkpk/test1/1_5.txt

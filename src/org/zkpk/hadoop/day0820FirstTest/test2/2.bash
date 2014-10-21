#!/bin/bash
sed 's/省//g' -i  china.txt
sed 's/自治区//g' -i china.txt
sed 's/特别行政区//g' -i china.txt
sed 's/维吾尔//g' -i china.txt
sed 's/回族//g' -i china.txt
sed 's/壮族//g' -i china.txt

sed 's/，/\n/g' -i china.txt
sed -n '/省/p' china.txt | sed 's/省//g' >x.txt


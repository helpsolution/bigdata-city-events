#!/bin/sh

mvn clean install

dir=$(( RANDOM ))

yarn jar target/bigdata_hadoop-HW1.jar Handler /user/hadoop/input_logs /user/hadoop/input_region /user/hadoop/output/$dir

hadoop fs -text /user/hadoop/output/$dir/part-r-00000.snappy > /tmp/output.txt
hadoop fs -text /user/hadoop/output/$dir/part-r-00001.snappy >> /tmp/output.txt
hadoop fs -text /user/hadoop/output/$dir/part-r-00002.snappy >> /tmp/output.txt

echo "Result:"

cat /tmp/output.txt

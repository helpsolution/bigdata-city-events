#!/bin/sh

hdfs dfs -mkdir /user/hadoop
hdfs dfs -mkdir /user/hadoop/input_logs /user/hadoop/input_region

hdfs dfs -put /usr/data/hw1/logs/* /user/hadoop/input_logs
hdfs dfs -put /usr/data/hw1/region/* /user/hadoop/input_region
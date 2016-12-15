#!/bin/sh
#
# auth:xiaopeng4
# date:2016-02-15
#

if [ -z "$1" -o -z "$2" ]; then
        echo "usage: $0 <port> <binlog number>"
        exit 0
fi

port=$1
binlog_number=$2
date=`date`

now_bilong_number=`ls /data1/mysql$port/mysql-bin.* | wc -l`

if [[ "$now_bilong_number" -gt "$binlog_number" ]]; then
        #count binlog number difference
        echo "$date $port 's binlog is $now_bilong_number more than $binlog_number,now will be purge ..."
        diff_num=$(($now_bilong_number - $binlog_number))
        last_binlog=`cd /data1/mysql$port ; ls mysql-bin.* | sort | head -n $diff_num | sort -r | head -n 1`

        if [[ "$last_binlog" = "" ]]; then
                echo "Can't find the last binlog, something is wrong, quit ..."
                exit 0
        fi

        #purge binlog
        echo $last_binlog
        /etc/dbCluster/mysqlha_login.sh -P $port -e"purge master logs to '$last_binlog' ;"
        echo "$date $port  purge binlog to $last_binlog" >> /data1/dbatemp/purge_binlog.logs

else
        echo "$date $port 's binlog is $now_bilong_number less then $binlog_number,do nothing ..."
        exit 0
fi



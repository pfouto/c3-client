#!/bin/sh

log_file=$1
shift


mkdir -p $(dirname "${log_file}")

echo "$(date) Running client jar..." > /proc/1/fd/1

echo "$@" > /proc/1/fd/1

#java -jar client.jar "$@" > /proc/1/fd/1 2>&1
java -Xmx3g -cp client.jar site.ycsb.Client -t -s -P config.properties "$@" 2> ${log_file}.err 1> ${log_file}.out
#java -cp client.jar site.ycsb.Client -t -s -P config.properties "$@" > /proc/1/fd/1 2>&1


#| tee ${log_file}
echo "$(date) Goodbye" > /proc/1/fd/1

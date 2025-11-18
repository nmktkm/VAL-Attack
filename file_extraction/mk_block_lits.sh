#!/usr/bin/bash

filename=$1

bunzip2 $filename
cat ${filename##*.} | awk -F ":" '{print $1}' | uniq > block_list.txt
cat ${filename##*.} | awk -F ":" '{print $1 " " $2}' | uniq > id_list.txt

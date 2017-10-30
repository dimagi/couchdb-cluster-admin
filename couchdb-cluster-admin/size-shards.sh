#!/usr/bin/env bash

# usage:
#   bash couchdb-cluster-admin/size-shards.sh <remote-ip> <remote-data-dir>

ip=$1
data_dir=$2
ssh ${ip} "cd ${data_dir}/shards; find . -type f | xargs ls -alS | xargs -L 1 echo | cut -d' ' -f5,9 | sed 's|\./||g'"

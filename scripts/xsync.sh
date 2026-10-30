#!/bin/bash
# 集群文件分发脚本：./xsync.sh /opt/module/hadoop/etc/hadoop
# 依赖 rsync，且需已配置 SSH 免密

if [ $# -lt 1 ]; then
  echo "用法: ./xsync.sh <要同步的绝对路径>"
  exit 1
fi

for host in hadoop102 hadoop104; do
  echo "---------- 分发到 $host ----------"
  rsync -av "$1" "$host:$1"
done

#!/bin/bash
# Zookeeper 集群群起脚本：./zk.sh start|stop|status
# 前提：已配置 hadoop101/102/104 的 SSH 免密登录

case $1 in
"start")
  for host in hadoop101 hadoop102 hadoop104; do
    echo "---------- zookeeper $host 启动 ----------"
    ssh $host "/opt/module/zookeeper/bin/zkServer.sh start"
  done
  ;;
"stop")
  for host in hadoop101 hadoop102 hadoop104; do
    echo "---------- zookeeper $host 停止 ----------"
    ssh $host "/opt/module/zookeeper/bin/zkServer.sh stop"
  done
  ;;
"status")
  for host in hadoop101 hadoop102 hadoop104; do
    echo "---------- zookeeper $host 状态 ----------"
    ssh $host "/opt/module/zookeeper/bin/zkServer.sh status"
  done
  ;;
*)
  echo "用法: ./zk.sh {start|stop|status}"
  ;;
esac

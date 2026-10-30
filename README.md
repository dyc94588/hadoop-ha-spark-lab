# Hadoop HA 与 Spark 集群搭建及词频统计实践

在 3 台 CentOS 7 虚拟机上从零搭建 **HDFS HA + YARN HA + Spark on YARN** 集群，并通过 HDFS 与本地两份文件的词频统计任务验证集群可用性，形成「分布式存储 — 资源调度 — 内存计算」的完整链路。

> 课程：《大数据实时数据分析》（2025-2026 学年度第一学期期末考核）
> 环境：CentOS 7 / JDK 1.8.0_212 / Hadoop 3.3.4 / Zookeeper 3.7.1 / Spark 3.3.1

---

## 一、集群规划

### 节点角色

| 节点 | HDFS HA | YARN HA | Zookeeper | Spark |
|---|---|---|---|---|
| hadoop101 | NameNode (active)、DataNode、JournalNode、ZKFC | ResourceManager (active)、NodeManager | Follower | Driver、Worker |
| hadoop102 | NameNode (standby)、DataNode、JournalNode、ZKFC | ResourceManager (standby)、NodeManager | Follower | Worker |
| hadoop104 | NameNode (standby)、DataNode、JournalNode、ZKFC | ResourceManager (standby)、NodeManager | Leader | Worker |

> 说明：NameNode 实际规划为 nn1 = hadoop101、nn2 = hadoop102 两节点 HA，hadoop104 与两台 NameNode 节点共同承担 DataNode / JournalNode / NodeManager / Zookeeper 角色，保证 JournalNode 三副本满足选举要求（奇数个）。

### 硬件与软件

- 每节点：2 核 CPU / 4GB 内存 / 50GB 存储，局域网互通
- 软件统一部署在 `/opt/module`，安装包在 `/opt/software`

| 软件 | 安装路径 | 配置/数据路径 |
|---|---|---|
| JDK | `/opt/module/jdk1.8.0_212` | — |
| Hadoop | `/opt/module/hadoop` | 配置 `etc/hadoop`，临时目录 `tmp` |
| Zookeeper | `/opt/module/zookeeper` | 配置 `conf`，数据 `data` |
| Spark | `/opt/module/spark` | 配置 `conf` |

### 端口规划

| 组件 | 端口 | 用途 |
|---|---|---|
| NameNode RPC | 9000 | HDFS 客户端通信 |
| NameNode Web UI | 9870 | HDFS Web 界面 |
| ResourceManager Web UI | 8088 | YARN 任务监控 |
| Zookeeper Client | 2181 | ZK 客户端连接 |
| JournalNode | 8485 | 元数据同步 |

---

## 二、搭建流程

### 1. 基础环境

```bash
# 1) 创建用户并授权
useradd dyc && echo "dyc:<password>" | chpasswd
echo "dyc ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
chown -R dyc:dyc /opt/module /opt/software

# 2) SSH 免密（在 hadoop101 上执行）
su - dyc
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
ssh-copy-id hadoop102
ssh-copy-id hadoop104

# 3) 环境变量（/etc/profile.d/my_env.sh）
export JAVA_HOME=/opt/module/jdk1.8.0_212
export HADOOP_HOME=/opt/module/hadoop
export ZOOKEEPER_HOME=/opt/module/zookeeper
export SPARK_HOME=/opt/module/spark
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$ZOOKEEPER_HOME/bin:$SPARK_HOME/bin
```

### 2. Zookeeper 集群

`conf/zoo.cfg` 关键配置（见 [`configs/zoo.cfg`](configs/zoo.cfg)）：

```properties
dataDir=/opt/module/zookeeper/data
server.101=hadoop101:2888:3888
server.102=hadoop102:2888:3888
server.104=hadoop104:2888:3888
```

每个节点的 `data/myid` 分别写 `101` / `102` / `104`，用 [`scripts/zk.sh`](scripts/zk.sh) 一键群起：

```bash
zk.sh start    # 或 stop / status
```

### 3. HDFS HA

核心改动（完整文件见 [`configs/`](configs)）：

**core-site.xml** — 命名服务与 ZK 地址

```xml
<property>
  <name>fs.defaultFS</name>
  <value>hdfs://mycluster</value>
</property>
<property>
  <name>ha.zookeeper.quorum</name>
  <value>hadoop101:2181,hadoop102:2181,hadoop104:2181</value>
</property>
```

**hdfs-site.xml** — 双 NameNode + JournalNode + ZKFC

```xml
<property><name>dfs.nameservices</name><value>mycluster</value></property>
<property><name>dfs.ha.namenodes.mycluster</name><value>nn1,nn2</value></property>
<property><name>dfs.namenode.rpc-address.mycluster.nn1</name><value>hadoop101:9000</value></property>
<property><name>dfs.namenode.rpc-address.mycluster.nn2</name><value>hadoop102:9000</value></property>
<property><name>dfs.namenode.shared.edits.dir</name>
  <value>qjournal://hadoop101:8485;hadoop102:8485;hadoop104:8485/mycluster</value></property>
<property><name>dfs.client.failover.proxy.provider.mycluster</name>
  <value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value></property>
<property><name>dfs.ha.automatic-failover.enabled</name><value>true</value></property>
```

启动顺序（**这步顺序错了最容易失败**）：

```bash
# 1) 三台都起 JournalNode
hdfs --daemon start journalnode

# 2) hadoop101 格式化并启动 NameNode
hdfs namenode -format
hdfs --daemon start namenode

# 3) hadoop102 同步元数据后启动（不要重复 format）
hdfs namenode -bootstrapStandby
hdfs --daemon start namenode

# 4) 格式化 ZKFC 并启动
hdfs zkfc -formatZK
hdfs --daemon start zkfc      # 各节点执行

# 5) 验证主备
hdfs haadmin -getServiceState nn1   # active
hdfs haadmin -getServiceState nn2   # standby
```

### 4. YARN HA

**yarn-site.xml** 关键项：

```xml
<property><name>yarn.resourcemanager.ha.enabled</name><value>true</value></property>
<property><name>yarn.resourcemanager.cluster-id</name><value>cluster-yarn1</value></property>
<property><name>yarn.resourcemanager.ha.rm-ids</name><value>rm1,rm2</value></property>
<property><name>yarn.resourcemanager.hostname.rm1</name><value>hadoop101</value></property>
<property><name>yarn.resourcemanager.hostname.rm2</name><value>hadoop102</value></property>
<property><name>yarn.resourcemanager.recovery.enabled</name><value>true</value></property>
<property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
```

**mapred-site.xml** 指定运行在 YARN 上，并配置历史服务地址。

```bash
yarn rmadmin -getServiceState rm1   # active
yarn rmadmin -getServiceState rm2   # standby
```

### 5. 词频统计验证

**HDFS 版（MapReduce）**

```bash
hdfs dfs -mkdir -p /test_input
hdfs dfs -put data/test_hdfs.txt /test_input
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.4.jar \
  wordcount /test_input /test_output
hdfs dfs -cat /test_output/part-r-00000
```

**本地文件版（Spark on YARN）**

```bash
spark-submit \
  --class org.apache.spark.examples.JavaWordCount \
  --master yarn \
  $SPARK_HOME/examples/jars/spark-examples_2.12-3.3.1.jar \
  file:///opt/module/test_local.txt
```

任务状态在 YARN Web UI（`http://hadoop101:8088`）查看，最终为 `FINISHED / SUCCEEDED`。

---

## 三、结果

- 3 节点 HDFS HA + YARN HA 集群正常启动，`hdfs haadmin` / `yarn rmadmin` 查询主备状态符合规划（hadoop101 为 active，hadoop102 为 standby）；
- 手动 `kill` active NameNode 后，ZKFC 经 Zookeeper 选举完成自动切换，HDFS 读写不中断；
- `test_hdfs.txt` 与 `test_local.txt` 两份词频统计结果准确，MapReduce 与 Spark 两条链路均跑通；
- Spark on YARN 任务可在 8088 界面实时监控，集群具备统一资源调度能力。

---

## 四、踩过的坑（比结论更有价值）

| 问题 | 原因 | 解决 |
|---|---|---|
| `hdfs namenode -format` 执行两次导致 clusterID 不一致，standby 无法同步 | 格式化会重置 `VERSION` 中的 clusterID | 只在 nn1 格式化一次，nn2 用 `-bootstrapStandby` 拉取元数据；必要时手工对齐 `dfs.namenode.name.dir/current/VERSION` |
| ZKFC 启动后两节点都变 standby | 未先 `hdfs zkfc -formatZK` 或 ZK 集群未就绪 | 先确认 `zk.sh status` 三节点正常，再格式化 ZKFC |
| JournalNode 起不来 / HA 切换失败 | 三台 `dfs.journalnode.edits.dir` 不一致或权限不对 | 统一路径为 `/opt/module/hadoop/data/journal`，`chown -R dyc:dyc` |
| `ssh hadoop102` 仍要密码 | 公钥同步到了错误用户目录 | 同一用户 `dyc` 下 `ssh-copy-id`，检查 `~/.ssh/authorized_keys` 权限 600 |
| spark-submit 报 `HADOOP_CONF_DIR` 未设置 | `spark-env.sh` 未同步到 worker 节点 | `spark-env.sh` 配置后 `xsync` 到三台 |
| WordCount 输出目录已存在报错 | HDFS 输出路径不可覆盖 | 先 `hdfs dfs -rm -r /test_output` |

---

## 五、收获

1. **HA 的本质是「共享元数据 + 选举」**：JournalNode 解决元数据多写，Zookeeper 解决谁当 active，ZKFC 是把两者串起来的执行者——把这三者关系理清，HA 就不再是背配置。
2. **配置一致性 > 配置正确性**：集群类实验 80% 的故障来自某个节点上的文件没同步、路径不一致或权限不对，`xsync` + 逐台 `diff` 是最省时间的排查手段。
3. **先验证单点，再叠加高可用**：先把单节点 Hadoop 跑通、再上 ZK、再上 HA，比一开始就三件套一起上更容易定位问题。
4. **同一份数据，两种计算模型**：MapReduce 走磁盘、Spark 走内存，用同一个词频任务对比，能直观理解「为什么 Spark 快」。

---

## 目录结构

```
hadoop-ha-spark-lab/
├── README.md                  # 本文
├── configs/                   # 关键配置文件（脱敏模板）
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── yarn-site.xml
│   ├── mapred-site.xml
│   └── zoo.cfg
├── scripts/
│   ├── zk.sh                  # Zookeeper 群起脚本
│   └── xsync.sh               # 集群文件分发脚本
├── data/
│   ├── test_hdfs.txt          # HDFS 词频统计输入样例
│   └── test_local.txt         # Spark 本地词频统计输入样例
└── docs/
    └── 实验报告原文.txt        # 课程实验报告原文（含截图位置说明）
```

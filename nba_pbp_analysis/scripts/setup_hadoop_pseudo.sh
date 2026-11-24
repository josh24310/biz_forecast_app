#!/usr/bin/env bash
# Hadoop Pseudo-Distributed 모드(단일 노드 HDFS) 설정/기동 스크립트
# Lab 환경에 이미 Hadoop 이 구성되어 있다면 이 스크립트는 건너뛰고 HDFS_URL 만 맞추면 된다.
set -euo pipefail
export HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop-3.4.1}
export JAVA_HOME=${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(which java)")")")}
HDFS_DATA=${HDFS_DATA:-/var/lib/hadoop-pseudo}
export HDFS_NAMENODE_USER=${HDFS_NAMENODE_USER:-$(whoami)}
export HDFS_DATANODE_USER=${HDFS_DATANODE_USER:-$(whoami)}

grep -q "^export JAVA_HOME=" "$HADOOP_HOME/etc/hadoop/hadoop-env.sh" || \
  echo "export JAVA_HOME=$JAVA_HOME" >> "$HADOOP_HOME/etc/hadoop/hadoop-env.sh"

cat > "$HADOOP_HOME/etc/hadoop/core-site.xml" <<EOF
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>$HDFS_DATA/tmp</value></property>
</configuration>
EOF
cat > "$HADOOP_HOME/etc/hadoop/hdfs-site.xml" <<EOF
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://$HDFS_DATA/name</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://$HDFS_DATA/data</value></property>
  <property><name>dfs.permissions.enabled</name><value>false</value></property>
</configuration>
EOF

if [ ! -d "$HDFS_DATA/name/current" ]; then
  "$HADOOP_HOME/bin/hdfs" namenode -format -nonInteractive -force
fi
# start-dfs.sh 는 ssh 가 필요하므로 데몬을 직접 기동
"$HADOOP_HOME/bin/hdfs" --daemon start namenode || true
"$HADOOP_HOME/bin/hdfs" --daemon start datanode || true
sleep 5
"$HADOOP_HOME/bin/hdfs" dfsadmin -safemode wait
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/nba
"$HADOOP_HOME/bin/hdfs" dfsadmin -report | head -12

#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-3.3.6}"
echo '=== Java ==='
java -version
javac -version
echo '=== Hadoop ==='
"$HADOOP_HOME/bin/hadoop" version
echo '=== Processes ==='
jps
echo '=== HDFS configuration ==='
for key in fs.defaultFS dfs.namenode.name.dir dfs.datanode.data.dir; do
  printf '%s: ' "$key"
  "$HADOOP_HOME/bin/hdfs" getconf -confKey "$key"
done
echo '=== HDFS health ==='
timeout 20s "$HADOOP_HOME/bin/hdfs" dfsadmin -report
echo '=== YARN nodes ==='
timeout 20s "$HADOOP_HOME/bin/yarn" node -list

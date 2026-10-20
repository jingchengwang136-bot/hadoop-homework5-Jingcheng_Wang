#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-3.3.6}"
HDFS="$HADOOP_HOME/bin/hdfs"
HADOOP="$HADOOP_HOME/bin/hadoop"
RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
BASE="/user/$(id -un)/homework5/$RUN_ID"
LOCAL="runs/$RUN_ID"
mkdir -p "$LOCAL"
test -f target/stock-set-jobs-1.0.jar || { echo 'First run: bash build.sh'; exit 1; }
"$HDFS" dfs -mkdir -p "$BASE/input"
"$HDFS" dfs -put input/A.txt input/B.txt "$BASE/input/"
printf '%s\n' "$BASE" > "$LOCAL/hdfs-path.txt"
"$HADOOP" jar target/stock-set-jobs-1.0.jar -Dmapreduce.framework.name=yarn \
  "$BASE/input/A.txt" "$BASE/input/B.txt" \
  "$BASE/output_task1" "$BASE/output_task2" 2>&1 | tee "$LOCAL/job.log"
"$HDFS" dfs -get "$BASE/output_task1" "$LOCAL/output_task1"
"$HDFS" dfs -get "$BASE/output_task2" "$LOCAL/output_task2"
python3 verify.py "$LOCAL/output_task1/part-r-00000" "$LOCAL/output_task2/part-r-00000"
mkdir -p output/output_task1 output/output_task2
cp "$LOCAL/output_task1/part-r-00000" output/output_task1/
cp "$LOCAL/output_task2/part-r-00000" output/output_task2/
cat output/output_task2/part-r-00000
echo "RUN_OK: $LOCAL"
echo 'Capture both hw5 applications as SUCCEEDED at http://localhost:8088/cluster'

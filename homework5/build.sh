#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-3.3.6}"
HADOOP="$HADOOP_HOME/bin/hadoop"
if [[ ! -x "$HADOOP" ]]; then echo "Hadoop not found: $HADOOP" >&2; exit 1; fi
mkdir -p .build/classes target
javac --release 8 -encoding UTF-8 -classpath "$("$HADOOP" classpath --glob)" \
  -d .build/classes src/main/java/edu/homework/StockSetJobs.java
jar cfe target/stock-set-jobs-1.0.jar edu.homework.StockSetJobs -C .build/classes .
echo "BUILD_OK: target/stock-set-jobs-1.0.jar"

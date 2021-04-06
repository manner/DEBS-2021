#!/bin/bash

checkpointingInterval=300000
batchSize=10000
benchmarkType="test"
benchmarkNamePrefix="testrun "

# stop script on failure
set -e

# working directory
cd $HOME

# ip of job manager
mainIP=192.168.1.27
# get ip of machine
curIP=$(hostname -I | cut -d' ' -f1)

# check and eventually set missing env variables
if [ -z "$FLINK_PROPERTIES" ]; then
  export FLINK_PROPERTIES="jobmanager.rpc.address: $(mainIP)"
fi
if [ -z "$CHECKPOINTING_INTERVAL" ]; then
  export CHECKPOINTING_INTERVAL="$checkpointingInterval"
fi
if [ -z "BATCH_SIZE" ]; then
  export BATCH_SIZE="$batchSize"
fi
if [ -z "BENCHMARK_TYPE" ]; then
  export BENCHMARK_TYPE="$benchmarkType"
fi
if [ -z "BENCHMARK_NAME_PREFIX" ]; then
  export BENCHMARK_NAME_PREFIX="benchmarkNamePrefix"
fi
if [ -z "$DEBS_API_KEY" ]; then
  echo "Error: DEBS_API_KEY not set!"
  return 1
fi

# install java if missing
if [ ! -n "$(which java)" ]; then
  sudo apt install openjdk-11-jre-headless
fi

# install flink
if [ ! -f flink.tgz ]; then
  wget -O flink.tgz https://apache.mirror.digionline.de/flink/flink-1.12.2/flink-1.12.2-bin-scala_2.12.tgz
fi

if [ ! -f flink-1.12.2/bin/flink ]; then
  tar zxvf flink-1.12.2-bin-scala_2.12.tgz
fi

# setup flink environment
if [ -z "$FLINK_HOME" ]; then
  export FLINK_HOME="$HOME/flink-1.12.2"
fi

# start task manager and eventually job manager if not running
if [ -z "$(top -n 1 | grep flink)" ]; then
  if [ "$mainIP" = "$curIP" ]; then
    exec "$FLINK_HOME/bin/taskmanager.sh" start
    exec "$FLINK_HOME/bin/jobmanager.sh" start
  else
    exec "$FLINK_HOME/bin/taskmanager.sh" start-foreground
  fi
fi

# run our pipeline
if [ "$mainIP" = "$curIP" ]; then
  if [ ! -f DEBS-2021-1.0-SNAPSHOT-all.jar ]
    echo "Error: Executable jar is missing!"
    return 1
  fi

  exec "$FLINK_HOME/bin/flink" run DEBS-2021-1.0-SNAPSHOT-all.jar
fi

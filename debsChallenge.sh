#!/usr/bin/env bash

# stop script on failure
set -e

# if running bash
if [ -n "$BASH_VERSION" ]; then
  echo "Started to set up bash environment"
  # include .profile if it exists
  if [ -f "$HOME/.bashrc" ]; then
    . "$HOME/.bashrc"
  fi
else
  echo "Skipped setup of bash environment"
fi

# get some infos about this script
scriptDir=$(dirname "$0")
scriptPath="$0"
scriptName=$(basename -- "$0")

# check if we want to upload this script to the cluster
deploy="false"
run="false"
deployScripts="false"
deployJar="false"
stop="false"
help="false"
runInternal="false"
internalStop="false"
while test $# -gt 0
do
  case "$1" in
    deploy) deploy="true"
      ;;
    run) run="true"
      ;;
    deployScripts) deployScripts="true"
      ;;
    deployJar) deployJar="true"
      ;;
    stop) stop="true"
      ;;
    help) help="true"
      ;;
    runInternal) runInternal="true"
      ;;
    internalStop) internalStop="true"
      ;;
  esac
  shift
done

if [ "$help" = "true" ]; then
  echo "Deployment script for the hpi debs pipeline";
  echo "";
  echo "Make sure that you have a ssh key for the VMs that you can run without";
  echo "otherwise you will have to enter the password pretty often";
  echo "";
  echo "Usage:";
  echo "  DEBS_API_KEY=<yourApiKeyHere> bash $scriptName [Options]";
  echo "";
  echo "Options:";
  echo "  deploy         - Deploys the debs challenge pipeline to the flink cluster.";
  echo "                   Make sure that you build the jar with 'gradle shadowJar'.";
  echo "  run            - Runs the pipeline on the cluster.";
  echo "  deployScripts  - Deploys this script and restarts flink cluster.";
  echo "  deployJar      - Deploys jar to the cluster.";
  echo "  stop           - Turns of the flink cluster.";
  echo "  help           - Prints this help text.";
  exit 0
fi

if [ "$deploy" = "true" ]; then
  deployScripts="true";
  deployJar="true";
fi

if [ -z "$DEBS_API_KEY" ]; then
  echo "Error: DEBS_API_KEY not set!"
  sleep 30
  exit 1
fi

# ip of job manager
mainIP="192.168.1.27"
# cluster ports
jobmanagerPort=10017
ports="$jobmanagerPort 10018 10019" # 10020 10021 10022 10023 10024 10025" # last need to be client that runs jar
# all editable parameters
debsApiKey="$DEBS_API_KEY"
checkpointingInterval="-1" # 180000
parallelism=25
numberOfNodes=0
numberOfTaskSlots=0 # 0 for auto computation
batchSize=10000
nrOfBatches="-1"
benchmarkType="Evaluation"
benchmarkNamePrefix="cluster test run "
for i in $ports; do
  ((numberOfNodes+=1))
done
if [ $numberOfTaskSlots = 0 ]; then
  if [ $((parallelism<numberOfNodes)) = 1 ]; then
    numberOfTaskSlots=1
  else
    ((numberOfTaskSlots=parallelism/numberOfNodes));
    if [ ! $((parallelism%numberOfNodes)) = 0 ]; then
      ((numberOfTaskSlots+=1))
    fi
  fi
fi

# get ip of machine
curIP=$(hostname -I | cut -d' ' -f1)

# scripts deployment
if [ "$deployScripts" = "true" ] || [ "$stop" = "true" ]; then
  echo "Started to setup cluster"
  for i in $ports; do
    if [ "$stop" = "true" ]; then
      ssh -p "$i" group-19@challenge.msrg.in.tum.de "DEBS_API_KEY=$debsApiKey screen -S stop -d -m bash $scriptName internalStop"
    else
      scp -P "$i" "$scriptPath" "group-19@challenge.msrg.in.tum.de:$scriptName";
      if [ "$i" = "$jobmanagerPort" ]; then
        ssh -p "$i" group-19@challenge.msrg.in.tum.de "DEBS_API_KEY=$debsApiKey screen -S jobmanager -d -m bash $scriptName"
      else
        ssh -p "$i" group-19@challenge.msrg.in.tum.de "DEBS_API_KEY=$debsApiKey screen -S taskmanager -d -m bash $scriptName"
      fi
    fi
  done;
  if [ "$run" = "false" ] && [ "$deployJar" = "false" ]; then
    echo "Finished"
    exit 0
  fi
else
  echo "Skipped setup of cluster"
fi

if [ "$deployJar" = "true" ]; then
  # deploy jar to cluster
  if [ ! -f "$scriptDir/build/libs/DEBS-2021-1.0-SNAPSHOT-all.jar" ]; then
    echo "Error: Executable $scriptDir/build/libs/DEBS-2021-1.0-SNAPSHOT-all.jar is missing!";
    exit 1
  else
    echo "Started to deploy jar to cluster"
    scp -P $jobmanagerPort "$scriptDir/build/libs/DEBS-2021-1.0-SNAPSHOT-all.jar" "group-19@challenge.msrg.in.tum.de:DEBS-2021-1.0-SNAPSHOT-all.jar"
  fi
  if [ "$run" = "false" ]; then
    echo "Finished"
    exit 0
  fi
else
  echo "Skipped deploying jar as it could not be fund under $scriptDir/build/libs/DEBS-2021-1.0-SNAPSHOT-all.jar"
fi

if [ "$run" = "true" ]; then
  echo "Started to run the debs pipeline";
  ssh -p $jobmanagerPort group-19@challenge.msrg.in.tum.de "DEBS_API_KEY=$debsApiKey screen -L -S app -d -m bash $scriptName runInternal";
  echo "Login to the cluster with 'ssh -p $jobmanagerPort group-19@challenge.msrg.in.tum.de' and";
  echo "login with 'screen -r app'. To detach from the screen without stopping it press 'ctrl + a'";
  echo "and then 'd'."
  exit 0
else
  echo "Skipped running debs pipeline"
fi

# working directory
echo "Changing working directory to $HOME"
cd "$HOME"

# check and eventually set missing env variables
echo "Started to set up environment variables"
if [ -z "$DEBS_API_KEY" ]; then
  export DEBS_API_KEY="$debsApiKey"
fi
if [ -z "$CHECKPOINTING_INTERVAL" ]; then
  export CHECKPOINTING_INTERVAL="$checkpointingInterval"
fi
if [ -z "$PARALLELISM" ]; then
  export PARALLELISM="$parallelism"
fi
if [ -z "$BATCH_SIZE" ]; then
  export BATCH_SIZE="$batchSize"
fi
if [ -z "$NR_OF_BATCHES" ]; then
  export NR_OF_BATCHES="$nrOfBatches"
fi
if [ -z "$BENCHMARK_TYPE" ]; then
  export BENCHMARK_TYPE="$benchmarkType"
fi
if [ -z "$BENCHMARK_NAME_PREFIX" ]; then
  export BENCHMARK_NAME_PREFIX="$benchmarkNamePrefix"
fi
if [ -z "$FLINK_PROPERTIES" ]; then
  export FLINK_PROPERTIES="jobmanager.rpc.address: $mainIP
jobmanager.rpc.port: 6123
jobmanager.memory.process.size: 1600m
taskmanager.memory.process.size: 8g
taskmanager.memory.task.off-heap.size: 1g
taskmanager.memory.jvm-metaspace.size: 1g
taskmanager.numberOfTaskSlots: $numberOfTaskSlots
parallelism.default: $parallelism
jobmanager.execution.failover-strategy: region"
fi

# install java if missing
if [ -z "$(which java)" ]; then
  echo "Started to install java"
  sudo apt -y install openjdk-11-jre-headless
else
  echo "Skipped Java install"
fi

# setup flink environment
if [ -z "$FLINK_HOME" ]; then
  echo "Started to setup flink environment"
  export FLINK_HOME="$HOME/flink-1.12.2"
else
  echo "Skipped flink environment setup"
fi

CONF_FILE="${FLINK_HOME}/conf/flink-conf.yaml"
CONF_FILE_BACKUP="${FLINK_HOME}/conf/flink-conf.yaml_backup"

# install flink
if [ ! -f flink-1.12.2/bin/flink ]; then
  echo "Started to install flink"
  if [ ! -f flink.tgz ]; then
    wget -O flink.tgz https://apache.mirror.digionline.de/flink/flink-1.12.2/flink-1.12.2-bin-scala_2.12.tgz
  fi

  tar zxvf flink.tgz
  cp "$CONF_FILE" "$CONF_FILE_BACKUP"
else
  echo "Skipped flink install"
fi

if [ "$runInternal" = "true" ]; then
  echo "Started to run the debs pipeline"
  "$FLINK_HOME"/bin/flink run DEBS-2021-1.0-SNAPSHOT-all.jar
  echo "Finished"
  exit 0
else
  echo "Skipped running debs pipeline"
fi

# override flink properties if provided
echo "Started to stopp flink cluster"
"$FLINK_HOME"/bin/taskmanager.sh stop-all;
"$FLINK_HOME"/bin/jobmanager.sh stop-all;
if [ -n "${FLINK_PROPERTIES}" ]; then
  echo "${FLINK_PROPERTIES}" > "${CONF_FILE}"
fi

if [ "$internalStop" = "true" ]; then
  exit 0
fi

# start flink managers
echo "Started to start flink managers on flink cluster"
if [ "$mainIP" = "$curIP" ]; then
  screen -S taskmanager -d -m "$FLINK_HOME"/bin/taskmanager.sh start-foreground
  "$FLINK_HOME"/bin/jobmanager.sh start-foreground
else
  "$FLINK_HOME"/bin/taskmanager.sh start-foreground
fi

echo "Finished"
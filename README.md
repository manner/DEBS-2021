# DEBS-2021 Challenge Project
Authors: Rafael Moczalla, Moritz Manner
Date: 08.03.2021

## Prerequisits

### Git and Java OpenJDK 11
To clone the project you need the revision tool Git and for building and executing
need Java OpenJDK. The project was tested with Java version 11. One can install
both with the following commands `sudo apt install openjdk-11-jdk-headless git` on
Ubuntu.

### IntelliJ IDE
This project is build with the IntelliJ IDE. To install IntelliJ run the following
commands.
```
wget -O ideaIC-2020.3.2.tar.gz https://download.jetbrains.com/idea/ideaIC-2020.3.2.tar.gz?_ga=2.189001837.737502174.1614963016-977816646.1614963016
sudo tar -xzf ideaIC-2020.3.2.tar.gz -C /opt
echo $'\n# add IntelliJ to path\nexport PATH=$PATH:/opt/idea-IC-203.7148.57/bin' >> ~/.bashrc
export PATH=$PATH:/opt/idea-IC-203.7148.57/bin
```

## Import the Project in IntelliJ
1. After cloning the project open the root folder of the project with the via 
   `File -> Open...`. IntelliJ will automatically detect the gradle build file 
   and prepare the project for you.
   
2. Build the project by right clicking on the root folder in the `Project` window
   on the left side of the IDE and selecting `Build module DEBS-2021`. All
   dependencies are downloaded and the project is build.
   
3. Run the main class to run your code. Open the main class by clicking on
   `src/java/main/de/hpi/debs/Main.java` in the `Project` window. One can run the
   main class by right clicking into the file and selecting `Run Main.main()`.

### Usage
Our solution for the DEBS 2021 Grand Challenge is developed ontop of Flink. There are
several options to run the solution. In any case you will need to setup several
environment variables to run the soluton successfully. An example of environment
variables tuning is shown in the following example in IntelliJ style.
`DEBS_API_KEY=YourDebsChallengeApiKeyHere;CHECKPOINTING_INTERVAL=0;PARALLELISM=5;BATCH_SIZE=10000;BENCHMARK_TYPE=Evaluation;BENCHMARK_NAME_PREFIX=SomePrefix;NR_OF_BATCHES=0`
The `CHECKPOINTING_INTERVAL` sets the interval of checkpointing and a value of 0
disables checkpointing. The `PARALLELISM` sets the number of parallelisation for. In
case you run our of memory you can add additional physiccal nodes to your cluster and
increase the parallelisation. A good setup is setting `PARALLELISM` to the number of
physical nodes running a Flink taskmanager. The `BATCH_SIZE` sets the size of the
batches our solution will request. The `NR_OF_BATCHES` sets the number of batches
you like to receive and a value of 0 disables. When `NR_OF_BATCHES` is disabled our
solution will continue to request new batches until the final batchs is received.
The batches contain a flag which determines wether or not it is the final batch.

## Stand Alone Execution on a Cluster
To run our solution on a real cluster we need first to setup the cluster. First, each
node needs to install Flink and start the taskmanager. Additionally, one node needs
to start the jobmanager.

A runable standalone jar package can be generated with `gradle clean shadowJar`. This
is supported by IntelliJ as well. After generating the jar we need to upload the jar
to the same node as the jobmanager. The solution can be startet with `flink run`.

We provide for the DEBS 2021 Grand Challenge a script that automates this procedures.
The `debsChallenge.sh` offers a `help` option that briefly describe
the script. A usual usage will look like follows. First, make sure that you logged
into each node per ssh at least once. Otherwise the script will no succeed as you are
usualy requested if you would like to add the identity of the ssh connection to your
known connections. Afterward, you will run
`DEBS_API_KEY=YourDebsChallengeApiKeyHere bash ./debsChallenge.sh deploy` to upload
the script and the jars to the cluster. Then, you can run our solution with
`DEBS_API_KEY=YourDebsChallengeApiKeyHere bash ./debsChallenge.sh run`. The script
will log into the node of the jobmanager and start our solution. The solution is run
in a seperate linux `screen`.

If you would like to use the flink dashboard to monitor our solution you can forward
a port via ssh to your loca machine and open `localhost:8081` in a web browser. If
a taskmanger crashes and do not restart automatically you shoud run
`DEBS_API_KEY=YourDebsChallengeApiKeyHere bash ./debsChallenge.sh stop` followed by
`DEBS_API_KEY=YourDebsChallengeApiKeyHere bash ./debsChallenge.sh deployScripts`.
This will first try to stop the Flink cluster. The second command will deploy the
script to the cluster again and start the Flink cluster once again.

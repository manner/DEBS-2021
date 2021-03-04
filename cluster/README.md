#Flink Cluster for Local Use
Author: Rafael Moczalla

Create Date: 1 March 2021

Last Update: 1 March 2021

Tested with Docker in version 20.10.4 and Docker Compose in version 1.28.5 on Ubuntu 18.04.2 LTS

##Prerequisites
To install Docker run

    ```
    sudo apt-get remove docker docker-engine docker.io containerd runc
    sudo apt-get update
    sudo apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
    sudo add-apt-repository \
    "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) \
    stable"
    sudo apt-get update
    sudo apt-get install docker-ce docker-ce-cli containerd.io
    sudo usermod -aG docker $USER
    ```

Test if docker ist working with the following command

    `sudo docker run hello-world`

To install Docker Compose run

    ```
    sudo curl -L "https://github.com/docker/compose/releases/download/1.28.5/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
    ```

##Start/Stop the Cluster
To start the cluster just run the following command in the same folder as the docker-compose file

    `docker-compose up -d --scale taskmanager=2`

To change the number of taskmanager nodes change the 2 to your desired number of nodes.

To access the dashboard type `127.0.0.1:8081` into your Browser.

To stop the cluster run the following command in the same folder as the docker-compose file.

    `docker-compose stop`

##Submit a Job to the Cluster
To submit a job just download a version of Flink and use it to submit a job as follows.

Dowwnload and unzip Flink

    `curl https://downloads.apache.org/flink/flink-1.12.1/flink-1.12.1-bin-scala_2.12.tgz | tar -xz`

Change into the directory and submit an example job as follows

    ```
    cd flink-1.12.1
    ./bin/flink run ./examples/streaming/TopSpeedWindowing.jar
    ```

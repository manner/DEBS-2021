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
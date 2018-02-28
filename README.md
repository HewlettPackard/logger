### Logger
A log parsing engine written in Java for high performance. Additionally, logger is also highly configurable.

### Logger Architecture
![ArchitectureImage](https://github.com/HewlettPackard/logger/blob/master/architecture.png "Logger Architecture")

### Log Parsing Performance

### Pre-requisites(Installing Maven)
You can download and install Maven from its official site - http://maven.apache.org/download.cgi

Here are the steps I did to install Maven on my Mac and VM:
  - ```$ wget http://mirror.cc.columbia.edu/pub/software/apache/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz```
  - ```$ sudo tar xzf apache-maven-3.2.5-bin.tar.gz -C /usr/local```
  - ```$ cd /usr/local```
  - ```$ sudo ln -s apache-maven-3.2.5 maven```
  - Add the Maven Path to the following file:
    - ```$ sudo vi /etc/profile.d/maven.sh```
    - ```export M2_HOME=/usr/local/maven```
    - ```export PATH=${M2_HOME}/bin:${PATH}```
  - Finally log out and log back in or source your profile and try the following command to verify if you have installed maven correctly
    - ```$ mvn -version```

### Running Logger Tests
Logger tests can be run from the command prompt using the maven build tool or on any Java specific editor like IntelliJ.
Logger uses testng(http://testng.org/doc/index.html) as its test framework.
Logging is disabled during testing. If you need to enable logging during testing edit the following file:
```
src/test/resources/log4j.xml
```
Change the level value from off to info/error

### To run all logger tests
```
mvn test
```

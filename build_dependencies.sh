#!/bin/sh
set -e
mkdir -p tmp/
if [ ! -e tmp/hadoop-snappy ]; then
  git clone https://github.com/electrum/hadoop-snappy.git tmp/hadoop-snappy
  cd tmp/hadoop-snappy && mvn install -Dsnappy.prefix="/usr/local/Cellar/snappy/1.1.7_1" && cd -
fi
if [ ! -e tmp/hadoop-mapred-0.22.0.jar ]; then
  curl -o tmp/hadoop-mapred-0.22.0.jar http://central.maven.org/maven2/org/apache/hadoop/hadoop-mapred/0.22.0/hadoop-mapred-0.22.0.jar
fi
if [ ! -e tmp/hadoop-mapred-0.22.0.pom ]; then
  curl -o tmp/hadoop-mapred-0.22.0.pom http://central.maven.org/maven2/org/apache/hadoop/hadoop-mapred/0.22.0/hadoop-mapred-0.22.0.pom
fi
cd tmp && mvn install:install-file -Dfile=hadoop-mapred-0.22.0.jar -DpomFile=hadoop-mapred-0.22.0.pom && cd -


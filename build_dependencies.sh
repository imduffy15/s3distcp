#!/bin/sh
set -e
mkdir -p tmp/
git clone https://github.com/electrum/hadoop-snappy.git tmp/hadoop-snappy
cd tmp/hadoop-snappy && mvn install && cd -


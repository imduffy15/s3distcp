#!/bin/sh
set -e
mkdir -p tmp
if [ ! -e tmp/procyon.jar ]; then
  curl -L -o tmp/procyon.jar https://bitbucket.org/mstrobel/procyon/downloads/procyon-decompiler-0.5.27.jar
fi
if [ ! -e tmp/s3distcp.jar ]; then
  curl -L -o tmp/s3distcp.jar https://elasticmapreduce.s3.amazonaws.com/libs/s3distcp/1.latest/s3distcp.jar
fi
java -jar tmp/procyon.jar -jar tmp/s3distcp.jar -o dist
echo "Printing difference of repo to official"
find dist/com/amazon -name "*.java" | grep -oE "com.+" | xargs -I{} diff dist/{} src/main/java/{}

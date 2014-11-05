This is a version of S3DistCp decompiled from the original JAR available from Amazon, with a guess at what the correct dependencies are. Primarily this is here to allow understanding how the program works, and potentially allowing for future modification. Scripts are included to decompile the latest version available from Amazon and compare it to the code in the repo.

This is not currently in production use anywhere, so use with extreme caution.

= Build Instructions

1. Ensure you have maven installed
1. Ensure you have snappy and lzo libs installed on your machine
1. Run `./build_dependencies.sh` to get the `hadoop-snappy` and `hadoop-mapred` jars installed into your Maven repo
1. Run `mvn compile`

#!/bin/bash 
# This is our template for running Avro MR jobs.
export HDP_PATH=`hadoop classpath`
export HADOOP_CLIENT_OPTS="-classpath $HDP_PATH:lib/*"
# You need to pass in -libjars first before your command-line arguments so that the ToolRunner can see what jars you need to include with the app
export PREFIX=""
export LIBJARS=""
for f in `ls -1 lib`
do
	export f="lib/$f"
	export LIBJARS=${LIBJARS}${PREFIX}${f} 
	export PREFIX=","
done

exec hadoop jar target/parmr-1.0-SNAPSHOT.jar com.github.sandgorgon.parmr.Main -libjars $LIBJARS $*
 

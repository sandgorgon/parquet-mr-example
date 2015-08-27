# parquet-mr-example
Avro Parquet MapReduce Implementation Example

Shows the following things:
- predicate pushdown
- column projection
- basic MR Job set-up

## Sample Parquet File
	user.parquet

## Steps to execute the app once built via Maven:

- set the HADOOP_CLASSPATH to include the location where the Parquet jars can be found.
- execute the app as follows:

	yarn jar parmr-1.0-SNAPSHOT.jar com.github.sandgorgon.parmr.Main -libjars avro-1.7.6.jar,avro-mapred-1.7.6-hadoop2.jar,parquet-avro-1.7.0.jar,parquet-column-1.7.0.jar,parquet-common-1.7.0.jar,parquet-format-2.3.0-incubating.jar,parquet-format-2.2.0-rc1.jar,parquet-generator-1.7.0.jar,parquet-hadoop-1.7.0.jar,parquet-jackson-1.7.0.jar,parquet-encoding-1.7.0.jar /user/sandgorgon/user.parquet /user/sandgorgon/results.dir

## Expected Results

	You should get as a result:
	user1	no value
	user2	no value

	Since the code projected out the favorite color column (hence 'no value'), and we projected out any row where the favorite number was 2.



#!/bin/sh
# source this file in your automation test code.

source /usr/local/hadoop-2.0.2-alpha-gphd-2.0.1/libexec/hadoop-config.sh

#java -cp "$CLASSPATH":~/.m2/repository/junit/junit/4.8.2/junit-4.8.2.jar:./target/hawq-mapreduce-tool-1.0.0-tests.jar:./target/hawq-mapreduce-tool-1.0.0.jar:../hawq-mapreduce-common/target/hawq-mapreduce-common-1.0.0.jar:../hawq-mapreduce-ao/target/hawq-mapreduce-ao-1.0.0.jar:./lib/postgresql-9.2-1003-jdbc4.jar org.junit.runner.JUnitCore com.pivotal.hawq.mapreduce.HAWQAOInputFormatFeatureTest_Others
java -cp "$CLASSPATH":~/.m2/repository/junit/junit/4.8.2/junit-4.8.2.jar:./target/hawq-mapreduce-tool-1.0.0-tests.jar:./target/hawq-mapreduce-tool-1.0.0.jar:../hawq-mapreduce-common/target/hawq-mapreduce-common-1.0.0.jar:../hawq-mapreduce-ao/target/hawq-mapreduce-ao-1.0.0.jar:./lib/postgresql-9.2-1003-jdbc4.jar org.junit.runner.JUnitCore com.pivotal.hawq.mapreduce.HAWQAOInputFormatFeatureTest_SingleType
#java -cp "$CLASSPATH":~/.m2/repository/junit/junit/4.8.2/junit-4.8.2.jar:./target/hawq-mapreduce-tool-1.0.0-tests.jar:./target/hawq-mapreduce-tool-1.0.0.jar:../hawq-mapreduce-common/target/hawq-mapreduce-common-1.0.0.jar:../hawq-mapreduce-ao/target/hawq-mapreduce-ao-1.0.0.jar:./lib/postgresql-9.2-1003-jdbc4.jar org.junit.runner.JUnitCore com.pivotal.hawq.mapreduce.HAWQAOInputFormatFeatureTest_Tpch

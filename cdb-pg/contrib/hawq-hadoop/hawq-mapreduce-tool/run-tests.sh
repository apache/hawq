#!/bin/sh
## ======================================================================

##
## List of default test cases to run.  This can be overwritten with
## the FEATURE_TEST_LIST environment variable.
##

FEATURE_TEST_LIST=${FEATURE_TEST_LIST:=com.pivotal.hawq.mapreduce.HAWQAOInputFormatFeatureTest_SingleType \
                                       com.pivotal.hawq.mapreduce.HAWQAOInputFormatFeatureTest_MultiType \
                                       com.pivotal.hawq.mapreduce.HAWQAOInputFormatFeatureTest_Tpch \
                                       com.pivotal.hawq.mapreduce.HAWQAOInputFormatFeatureTest_Others }
##
## Default location for test working tmp directory.
##

HAWQ_MAPREDUCE_TOOL_TMPDIR=${HAWQ_MAPREDUCE_TOOL_TMPDIR:=/tmp/hawq-mapreduce-tool}

##
## Bring in hadoop config environment.
##

source /usr/lib/gphd/hadoop/libexec/hadoop-config.sh

##
## Cleanup previous tmp working directory
##

rm -rf ${HAWQ_MAPREDUCE_TOOL_TMPDIR}
mkdir -p ${HAWQ_MAPREDUCE_TOOL_TMPDIR}

##
## Populate tmp working directory
##

cp ../../demo_mapreduce/* ${HAWQ_MAPREDUCE_TOOL_TMPDIR}

##
## Clean up any previous tmp working directory and copy contents to
## remote systems.
##

for host in smdw sdw1 sdw2; do
    ssh ${host} rm -rf ${HAWQ_MAPREDUCE_TOOL_TMPDIR}
    scp -r ${HAWQ_MAPREDUCE_TOOL_TMPDIR} ${host}:${HAWQ_MAPREDUCE_TOOL_TMPDIR}
done

##
## Clean up any previous tmp working directory and copy contents to
## remote systems.
##

for jar in ls /usr/lib/gphd/hadoop-yarn/*.jar ${HAWQ_MAPREDUCE_TOOL_TMPDIR}/*.jar; do
    CLASSPATH=$CLASSPATH:${jar}
done

for test in ${FEATURE_TEST_LIST}; do
    command="java -cp $CLASSPATH org.junit.runner.JUnitCore ${test}"

    echo ""
    echo "======================================================================"
    echo "Timestamp ..... : $( date)"
    echo "Running test .. : ${test}"
    echo "command ....... : ${command}"
    echo "----------------------------------------------------------------------"

    ${command} | tee -a inputformat.logs 2>&1

    echo "======================================================================"
    echo ""
done

## 
## Generate readable test report
##
python generate_mr_report.py

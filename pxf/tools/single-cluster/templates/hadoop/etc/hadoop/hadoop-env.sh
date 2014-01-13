# load singlecluster environment
. $bin/../../bin/gphd-env.sh

export HADOOP_CLASSPATH=\
`echo $PXF_ROOT/pxf-core-*[0-9].jar`:\
`echo $PXF_ROOT/pxf-api-*[0-9].jar`:\
`echo $HBASE_ROOT/hbase-*-gphd-*[0-9].jar`:\
`echo $HBASE_ROOT/hbase-*-gphd-*[0-9]-SNAPSHOT.jar`:\
`echo $HBASE_ROOT/hbase-*-gphd-*[0-9]-SNAPSHOT-security.jar`:\
`echo $HBASE_ROOT/hbase-*-gphd-*[0-9]-security.jar`:\
`echo $ZOOKEEPER_ROOT/zookeeper-*-gphd-*[0-9].jar`:\
`echo $ZOOKEEPER_ROOT/zookeeper-*-gphd-*[0-9]-SNAPSHOT.jar`:\
`echo $HIVE_ROOT/lib/hive-service-*-gphd-*[0-9].jar`:\
`echo $HIVE_ROOT/lib/hive-service-*-gphd-*[0-9]-SNAPSHOT.jar`:\
`echo $HIVE_ROOT/lib/libthrift.jar`:\
`echo $HIVE_ROOT/lib/hive-metastore-*-gphd-*[0-9].jar`:\
`echo $HIVE_ROOT/lib/hive-metastore-*-gphd-*[0-9]-SNAPSHOT.jar`:\
`echo $HIVE_ROOT/lib/libfb303-*.jar`:\
`echo $HIVE_ROOT/lib/hive-common-*-gphd-*[0-9].jar`:\
`echo $HIVE_ROOT/lib/hive-common-*-gphd-*[0-9]-SNAPSHOT.jar`:\
`echo $HIVE_ROOT/lib/hive-exec-*-gphd-*[0-9].jar`:\
`echo $HIVE_ROOT/lib/hive-exec-*-gphd-*[0-9]-SNAPSHOT.jar`:\
`echo $PXF_ROOT/avro-[0-9]*[0-9].jar`:\
`echo $PXF_ROOT/avro-mapred-[0-9]*[0-9].jar`:\
`echo $PXF_ROOT/protobuf-java-*[0-9].jar`:\
$HBASE_CONF:\
$HIVE_CONF:\
$HADOOP_CLASSPATH:\
$COMMON_CLASSPATH:\

# Extra Java runtime options.  Empty by default.
export HADOOP_OPTS="$HADOOP_OPTS $COMMON_JAVA_OPTS"

export COMMON_MASTER_OPTS="-Dhadoop.tmp.dir=$HADOOP_STORAGE_ROOT"

# Command specific options appended to HADOOP_OPTS when specified
export HADOOP_NAMENODE_OPTS="$COMMON_MASTER_OPTS"
export HADOOP_SECONDARYNAMENODE_OPTS="$COMMON_MASTER_OPTS"

# Where log files are stored.  $HADOOP_HOME/logs by default.
export HADOOP_LOG_DIR=$LOGS_ROOT

# The directory where pid files are stored. /tmp by default.
export HADOOP_PID_DIR=$PIDS_ROOT

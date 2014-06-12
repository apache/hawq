# paths
export JAVA_HOME=${JAVA_HOME:=/Library/Java/Home}
export STORAGE_ROOT=$GPHD_ROOT/storage
export HADOOP_STORAGE_ROOT=$STORAGE_ROOT/hadoop
export ZOOKEEPER_STORAGE_ROOT=$STORAGE_ROOT/zookeeper
export HBASE_STORAGE_ROOT=$STORAGE_ROOT/hbase
export HIVE_STORAGE_ROOT=$STORAGE_ROOT/hive
export PXF_STORAGE_ROOT=$STORAGE_ROOT/pxf

# settings
export SLAVES=3

# Automatically start HBase during GPHD startup
export START_HBASE=true

# Automatically start Stargate during HBase startup
export START_STARGATE=false

# HBase REST service (Stargate) port
export STARGATE_PORT=60009

# Automatically start MapReduce
export START_YARN=true

# Automatically start MapReduce History Server
export START_YARN_HISTORY_SERVER=false

# Automatically start Hive Metastore server
export START_HIVEMETASTORE=true

# Automatically start PXF service
export START_PXF=false

# These settings go into all HBase's, Hadoop's JVMs
export COMMON_JAVA_OPTS=${COMMON_JAVA_OPTS}

# This classpath is automatically added to HBase's and Hadoop's classpaths
# remember to use ':' as separator
export COMMON_CLASSPATH=

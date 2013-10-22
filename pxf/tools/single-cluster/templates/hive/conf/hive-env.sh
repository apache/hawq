# load singlecluster environment
if [ -f $bin/../../bin/gphd-env.sh ]; then
	. $bin/../../bin/gphd-env.sh
fi

export HIVE_OPTS="-hiveconf hive.log.dir=$LOGS_ROOT -hiveconf derby.stream.error.file=$LOGS_ROOT/derby.log -hiveconf javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$HIVE_STORAGE_ROOT/metastore_db;create=true"
export HADOOP_HOME=$HADOOP_ROOT

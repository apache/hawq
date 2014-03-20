1. What is this?
================

SingleCluster for Hadoop stack 2.x of PHD

SingleCluster is a zero-configuration 3 node (up to 10) cluster
running on a single machine.

Not including administration nodes, by default the following services
will be started:

1 Zookeeper
1 NameNode
1 ResourceManager
1 HBase Master
3 Data Nodes
3 Region Servers
3 Node Managers
1 Hive Thrift server

Uses an unmodified version of relevant PHD services

2. Support
==========

dov.dorin AT gopivotal.com

3. Requirements
===============

1.  Oracle Java 1.7.x JDK
	(not JRE as scripts currently use jps, will be fixed in the future)

4. Setup
========

1.  JAVA_HOME must be defined in environment or set at conf/gphd-conf.sh
    (default set to /Library/Java/Home, should be good enough under OS X)

2.  Initialize the cluster:
	bin/init-gphd.sh

3.  Start GPHD: 
	bin/start-gphd.sh

5. Using
========

Use bin/hdfs, bin/hadoop, bin/hive, bin/hbase for commandline access

bin/start-* bin/stop-* will do as you expect

6. Known Issues
===============

General
-------
- No support for -upgrade / -rollback
- Maximum number of datanodes is 10

CentOS
------
- Need to disable IPv6 on CentOS
- Need to map your hostname to 127.0.0.1 in /etc/hosts

7. Changes
==========

v1.7.7
------

- JRE can be used instead of JDK
- GPSQL-1363: start-gphd.sh fails to recognize Zookeeper is up
- GPSQL-1987: use US dist server for refresh_tars

v1.7.6
------

- GPSQL-1558: mapreduce.input.fileinputformat.input.dir.recursive is set to true.
- GPSQL-1712: Pull stack from dist server (hdsh129) instead of Jenkins
- GPSQL-1723: Add pxf-hbase.jar to HBase's classpath
- GPSQL-1742: HBase classpath cleanup

v1.7.5
------

- GPSQL-1685: Tiding up SingleCluster classpath
- GPSQL-1647: Enable release-head and use stack-phd2.0-release
- GPSQL-1647: Use stack-phd2.0-release for dev-head
- GPSQL-1650: cp PXF tarballs instead of mv

v1.7.4
------

- GPSQL-1641: Use HudsonHD2_2_0HadoopStackBuild_Release

v1.7.3
------

- GPSQL-1618: yarn.nodemanager.aux-services value changed to mapreduce_shuffle.  
- GPSQL-1445: Use stack build from new Jenkins job
- tar fetch is now logged
- GPSQL-1374: reduce verbosity
- GPSQL-1258: Use multiple pxf-*

v1.7.2
------

- GPSQL-1235: Stop using jps tool
- GPSQL-1272: stop-hbase.sh sometimes hangs forever
- GPSQL-1363: Add a temporary delay before checking Zookeeper's status

v1.7.1
------

- added -f to rm in copy_templates, tars/release-head/Makefile and
  tars/dev-head/Makefile
- make clean doens't fail on rm -rf
- ENGINF-810: Please chmod 777 for DFS_URL/../ per the hdfs privilege 
  check involved in hawq
- GPSQL-1180: hbase jar file name changed in dev-stack

v1.7.0
------

- Adding Hive Metastore service
- Removing Hive Thrift server
- start-gphd.sh stops on subscript errors

v1.6.5
------

- changes in refresh_tars:
	- build fails when a component fetch fails
	- now taking pxf tarball from artifacts
- release-head uses release stack components

v1.6.4
------

- reverting dev-head to use dev stack components instead of release

v1.6.3
------

- fix new Jenkins compatibility
- GPSQL-943: Pull Hadoop dev stack on release (temporarily)

v1.6.2
------

- HD-6212: Temporary bypass, using release stack components

v1.6.1
------

- HD-5845: erroneous $ in init-gphd.sh message
- Better format for init-gphd.sh environment
- HD-6034: start-gphd.sh will fail start-hdfs.sh fails
- HD-6079: init-gphd.sh prints path without bin directory

v1.6
----

- Add init-gphd.sh to initialize/reinitialize cluster
- init-gphd.sh will print recommended environment vars
- JAVA_HOME can be taken from environment
- bin/hdfs, bin/hadoop, bin/hbase, bin/hive now run using same environment
  as hadoop/bin/hdfs, hadoop/bin/hadoop, hbase/bin/hbase, hive/bin/hive
- start-hdfs.sh will fail when HDFS not initialized
- Update README.txt
- Add restart-gphd.sh to ease cluster restart
- Fix issue with stop-hive.sh not really stopping hive

v1.5.1
------

- Remove GPHD 1.x support
- Fix tars/README.txt typo
- use single Jenkins fetch script
- Prepare for Jenkins job build
	- Add BUILD_NUMBER environment variable
	- add refresh_tars target to download latest tar files for a release
- bring back SNAPSHOT postfix :(
- Fix HD-5592: Hive and HBase tarballs are not fetched automatically 
  in SingleCluster build
- Fix HD-5595: PXF hive regressions fail: cannot access 
  com.facebook.fb303.FacebookService
- HD-5329: gpxf->pxf name change
- build output is always singlecluster.tar.gz

v1.5
----

- versions.txt contains version of all components
- Release directories to support multiple releases
- dev-head release refresh scripts
- release-head release refresh scripts
- GPXF_ROOT -> PXF_ROOT
- remove SNAPSHOT postfix

v1.5a
----

- versions
	- GPXF_dev b31
	- Hadoop_2x_ISRARD b6
	- HudsonHD2_x_HBaseBuild b67
	- HudsonHD2_x_HiveBuild b38
	- HudsonHD2_x_ZookeeperBuild b31
- hadoop-env.sh supports SNAPSHOT postfix
- fix typeos in start-hive.sh and stop-hive.sh

v1.4
----

- Moved FUSION_ROOT to GPXF_ROOT
- Moved gpfusion to gpxf (jar files)
- Fixed a bug in renaming tarballs to base form
- Added a new doc for building a single-cluster distribution

v1.3
----

- Common classpath
- Added Avro and Protobuf to Hadoop classpath
- Suitable for HAWQ/GPXF
- Start/stop script for Hive thrift server
- Move to 2x naming

v1.2
----

- Now using GPHD 2.0.1(M1)

v1.1.1
------

- Do not use seq command

v1.1
----

- Adding mapreduce (YARN) services
- Adding Hive support
- Produce proper error when no java
- Fix issues with running commands directly from bin
- stop-gphd.sh will stop HBase only when configured to
- Move default Stargate port to 60009 instead of 8081 (as McAfee's cma uses
  that on some macs)

v1.0
----

- Initial release

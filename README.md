![HAWQ](http://hawq.apache.org/images/logo-hawq.png)

---

|CI Process|Status|
|---|---|
|Travis CI Build|[![https://travis-ci.org/apache/hawq.svg?branch=master](https://travis-ci.org/apache/hawq.png?branch=master)](https://travis-ci.org/apache/hawq?branch=master)|
|Apache Release Audit Tool ([RAT](https://creadur.apache.org/rat/))|[![Rat Status](https://builds.apache.org/buildStatus/icon?job=HAWQ-rat)](https://builds.apache.org/view/HAWQ/job/HAWQ-rat/)|
|Coverity Static Analysis   |[![Coverity Scan Build](https://scan.coverity.com/projects/apache-hawq/badge.svg)](https://scan.coverity.com/projects/apache-hawq)|

---

[Website](http://hawq.apache.org/) |
[Wiki](https://cwiki.apache.org/confluence/display/HAWQ) |
[Documentation](http://hawq.apache.org/docs/userguide/latest/) |
[Developer Mailing List](mailto:dev@hawq.apache.org) |
[User Mailing List](mailto:user@hawq.apache.org) |
[Q&A Collections](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65144284) |
[Open Defect](https://issues.apache.org/jira/browse/HAWQ)


# Apache HAWQ
---
Apache HAWQ is a Hadoop native SQL query engine that combines the key technological advantages of MPP database with the scalability and convenience of Hadoop. HAWQ reads data from and writes data to HDFS natively. HAWQ delivers industry-leading performance and linear scalability. It provides users the tools to confidently and successfully interact with petabyte range data sets. HAWQ provides users with a complete, standards compliant SQL interface. More specifically, HAWQ has the following features:

 - On-premise or cloud deployment
 - Robust ANSI SQL compliance: SQL-92, SQL-99, SQL-2003, OLAP extension
 - Extremely high performance. many times faster than other Hadoop SQL engine
 - World-class parallel optimizer
 - Full transaction capability and consistency guarantee: ACID
 - Dynamic data flow engine through high speed UDP based interconnect
 - Elastic execution engine based on virtual segment & data locality
 - Support multiple level partitioning and List/Range based partitioned tables
 - Multiple compression method support: snappy, gzip, zlib
 - Multi-language user defined function support: Python, Perl, Java, C/C++, R
 - Advanced machine learning and data mining functionalities through MADLib
 - Dynamic node expansion: in seconds
 - Most advanced three level resource management: Integrate with YARN and hierarchical resource queues.
 - Easy access of all HDFS data and external system data (for example, HBase)
 - Hadoop Native: from storage (HDFS), resource management (YARN) to deployment (Ambari).
 - Authentication & Granular authorization: Kerberos, SSL and role based access
 - Advanced C/C++ access library to HDFS and YARN: libhdfs3 & libYARN
 - Support most third party tools: Tableau, SAS et al.
 - Standard connectivity: JDBC/ODBC

# Build & Setup HAWQ on Mac

## Step 1 Setup HDFS

Install HomeBrew referring to [here](https://brew.sh/).

```
brew install hadoop
```
### Step 1.1 Configure HDFS parameters

* `${HADOOP_HOME}/etc/hadoop/slaves`
	
	For example, `/usr/local/Cellar/hadoop/2.8.1/libexec/etc/hadoop/slaves`

	```
	localhost
	```

* `${HADOOP_HOME}/etc/hadoop/core-site.xml`

	For example, `/usr/local/Cellar/hadoop/2.8.1/libexec/etc/hadoop/core-site.xml`

	```xml
	<?xml version="1.0" encoding="UTF-8"?>
	<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
	<configuration>
	    <property>
	        <name>fs.defaultFS</name>
	        <value>hdfs://localhost:8020</value>
	    </property>
	</configuration>
	```

* `${HADOOP_HOME}/etc/hadoop/hdfs-site.xml`

	For example, `/usr/local/Cellar/hadoop/2.8.1/libexec/etc/hadoop/hdfs-site.xml`

	**Attention: Replace `${HADOOP_DATA_DIRECTORY}` and `${USER_NAME}` variables with your own specific values.**

	```xml
	<?xml version="1.0" encoding="UTF-8"?>
	<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
	<configuration>
	    <property>
	        <name>dfs.namenode.name.dir</name>
	        <value>file://${HADOOP_DATA_DIRECTORY}/name</value>
	        <description>Specify your dfs namenode dir path</description>
	    </property>
	    <property>
	        <name>dfs.datanode.data.dir</name>
	        <value>file://${HADOOP_DATA_DIRECTORY}/data</value>
	        <description>Specify your dfs datanode dir path</description>
	    </property>
	    <property>
	        <name>dfs.replication</name>
	        <value>1</value>
	    </property>
	</configuration>
	```

### Step 1.2 Configure HDFS environment

```bash
touch ~/.bashrc
touch ~/.bash_profile
	
echo "if [ -f ~/.bashrc ]; then
source ~/.bashrc
fi" >> ~/.bash_profile
	
echo "export HADOOP_HOME=/usr/local/Cellar/hadoop/2.8.1/libexec" >> ~/.bashrc
echo "export PATH=$PATH:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin" >> ~/.bashrc
	
source ~/.bashrc
```

### Step 1.3 Setup passphraseless ssh	
```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
```

Now you can `ssh localhost` without a passphrase. If you meet `Port 22 connecting refused` error, turn on `Remote login` in your Mac's `System Preferences->Sharing`.

### Step 1.4 Format the HDFS filesystem

```bash
hdfs namenode -format
```

### Step 1.5 Start HDFS

```bash
# start/stop HDFS
start-dfs.sh/stop-dfs.sh
 
# Do some basic tests to make sure HDFS works
hdfs dfsadmin -report
hadoop fs -ls /
```

**When things go wrong, check the log and view the FAQ in wiki first.**

## Step 2 Setup hawq

### Step 2.1 System configuration

#### Step 2.1.1 Turn off Rootless System Integrity Protection

Turning Off Rootless System Integrity Protection on macOS that newer than `OS X El Capitan 10.11` if you encounter some tricky LIBRARY_PATH problems, e.g. HAWQ-513, which makes hawq binary not able to find its shared library dependencies. Steps below:

1. Reboot the Mac and hold down Command + R keys simultaneously after you hear the startup chime, this will boot OS X into Recovery Mode 
2. When the “OS X Utilities” screen appears, pull down the ‘Utilities’ menu at the top of the screen instead, and choose “Terminal”
3. Type the following command into the terminal then hit return: csrutil disable; reboot

#### Step 2.1.2 Configure `sysctl.conf`

For Mac OSX 10.10 / 10.11, add following content to `/etc/sysctl.conf` and then `sudo sysctl -p` to activate them.

For Mac OSX 10.12+, add following content to `/etc/sysctl.conf` and then `cat /etc/sysctl.conf | xargs sudo sysctl` to check.

```
kern.sysv.shmmax=2147483648
kern.sysv.shmmin=1
kern.sysv.shmmni=64
kern.sysv.shmseg=16
kern.sysv.shmall=524288
kern.maxfiles=65535
kern.maxfilesperproc=65536
kern.corefile=/cores/core.%N.%P
```

### Step 2.2 Prepare source code and target folder

```bash
mkdir ~/dev
git clone git@github.com:apache/hawq ~/dev/hawq

sudo mkdir -p /opt
sudo chmod a+w /opt
sudo install -o $USER -d /usr/local/hawq
```
### Step 2.3 Setup toolchain and thirdparty dependency

Setup toolchain and thirdparty dependency

### Step 2.4 Build HAWQ

- 2.4.1 Add hawq environment information to `~/.bashrc`, and **`source ~/.bashrc`** to make it effect.

  ```bash
  ulimit -c 10000000000
  export CC=clang
  export CXX=clang++
  export DEPENDENCY_PATH=/opt/dependency/package
  source /opt/dependency-Darwin/package/env.sh
  ```
- 2.4.2 Build HAWQ

  ```bash
  cd ~/dev/hawq
  git checkout master
  ln -sf ../../commit-msg .git/hooks/commit-msg
  ./configure
  make -j8
  make -j8 install
  ```


### Step 2.5 Configure HAWQ

```shell
mkdir /tmp/magma_master
mkdir /tmp/magma_segment
```

Feel free to use the default `/usr/local/hawq/etc/hawq-site.xml`. Pay attention to mapping `hawq_dfs_url` to `fs.defaultFS` in `${HADOOP_HOME}/etc/hadoop/core-site.xml`.

### Step 2.6 Init/Start/Stop HAWQ

```bash
# Before initializing HAWQ, you need to install HDFS and make sure it works.
 
source /usr/local/hawq/greenplum_path.sh
 
# Besides you need to set password-less ssh on the systems.
# If only install hawq for developing in localhost, skip this step.
# Exchange SSH keys between the hosts host1, host2, and host3:
#hawq ssh-exkeys -h host1 -h host2 -h host3

# Initialize HAWQ cluster and start HAWQ by default
hawq init cluster -a

# Now you can stop/restart/start the cluster using:  
hawq stop/restart/start cluster
# Init command would invoke start command automaticlly too.
 
# HAWQ master and segments are completely decoupled.
# So you can also init, start or stop the master and segments separately.
# For example, to init: hawq init master, then hawq init segment
#              to stop: hawq stop master, then hawq stop segment
#              to start: hawq start master, then hawq start segment
```

> Everytime you init hawq you need to delete some files. The directory of all files you need to delete have been configured in /usr/local/hawq/etc/hawq-site.xml.
> 
> - 1) Name:`hawq_dfs_url` Description:URL for accessing HDFS
> - 2) Name:`hawq_master_directory` Description:The directory of hawq master
> - 3) Name:`hawq_segment_directory` Description:The directory of hawq segment
> - 4) Name:`hawq_magma_locations_master` Description:HAWQ magma service locations on master
> - 5) Name:`hawq_magma_locations_segment` Description:HAWQ magma service locations on segment
> 
> i.e.
> 
> ```bash
> hdfs dfs -rm -r /hawq*
> rm -rf /Users/xxx/data/hawq/master/*
> rm -rf /Users/xxx/data/hawq/segment/*
> rm -rf /Users/xxx/data/hawq/tmp/magma_master/*
> rm -rf /Users/xxx/data/hawq/tmp/magma_segment/*
> ```
> 
> Check whether there is any process of postgres or magma running in your computer. If they are running ,you must kill them before you init hawq. For example,
> 
> ```bash
> ps -ef | grep postgres | grep -v grep | awk '{print $2}'| xargs kill -9
> ps -ef | grep magma | grep -v grep | awk '{print $2}'| xargs kill -9
> ```

# Build HAWQ on Centos 7

Almost the same as that on macOS, feel free to have a try.

# Build HAWQ on Centos 7(6.X) using docker

Almost the same as that on macOS, feel free to have a try.

# Build & Install & Test (Apache HAWQ Version)

---------------
Please see HAWQ wiki page:
https://cwiki.apache.org/confluence/display/HAWQ/Build+and+Install

```shell
cd hawq
make feature-test
```

To make the output is consistent, please create a newdb and use specific locale.

```
TEST_DB_NAME="hawq_feature_test_db"
psql -d postgres -c "create database $TEST_DB_NAME;"
export PGDATABASE=$TEST_DB_NAME
psql -c  "alter database $TEST_DB_NAME set lc_messages to 'C';"
psql -c "alter database $TEST_DB_NAME set lc_monetary to 'C';"
psql -c  "alter database $TEST_DB_NAME set lc_numeric to 'C';"
psql -c  "alter database $TEST_DB_NAME set lc_time to 'C';"
psql -c "alter database $TEST_DB_NAME set timezone_abbreviations to 'Default';"
psql -c  "alter database $TEST_DB_NAME set timezone to 'PST8PDT';"
psql -c  "alter database $TEST_DB_NAME set datestyle to 'postgres,MDY';"
```

To run normal feature test , please use below filter:
1. Below tests  can only run in sequence mode
```
hawq/src/test/feature/feature-test --gtest_filter=-TestHawqRegister.*:TestTPCH.TestStress:TestHdfsFault.*:TestZookeeperFault.*:TestHawqFault.*
```
2. Below tests can run in parallel
```
cd hawq/src/test/feature/
mkdir -p testresult
python ./gtest-parallel --workers=4 --output_dir=./testresult --print_test_times  ./feature-test --gtest_filter=-TestHawqRegister.*:TestTPCH.*:TestHdfsFault.*:TestZookeeperFault.*:TestHawqFault.*:TestQuitQuery.*:TestErrorTable.*:TestExternalTableGpfdist.*:TestExternalTableOptionMultibytesDelimiter.TestGpfdist:TETAuth.*
```

  TestHawqRegister is not included
  TestTPCH.TestStress is for TPCH stress test
  TestHdfsFault       Hdfs fault tests 
  TestZookeeperFault  Zookeeper fault tests      
  TestHawqFault       Hawq fault tolerance tests


# Export Control
----------------

This distribution includes cryptographic software. The country in which you
currently reside may have restrictions on the import, possession, use, and/or
re-export to another country, of encryption software. BEFORE using any
encryption software, please check your country's laws, regulations and
policies concerning the import, possession, or use, and re-export of encryption
software, to see if this is permitted. See <http://www.wassenaar.org/> for more
information.

The U.S. Government Department of Commerce, Bureau of Industry and Security
(BIS), has classified this software as Export Commodity Control Number (ECCN)
5D002.C.1, which includes information security software using or performing
cryptographic functions with asymmetric algorithms. The form and manner of this
Apache Software Foundation distribution makes it eligible for export under the
License Exception ENC Technology Software Unrestricted (TSU) exception (see the
BIS Export Administration Regulations, Section 740.13) for both object code and
source code.



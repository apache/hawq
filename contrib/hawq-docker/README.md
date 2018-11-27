# hawq-docker

hawq-docker is based on *wangzw's* repo *hawq-devel-env*. It is the docker images and scripts to help developers of Apache HAWQ to setup building and testing environment with docker.

Both CentOS 7 and CentOS 6 are supported.
Change variable **OS_VERSION** (:= centos7 OR centos6) in Makefile to switch between CentOS 7 and CentOS 6.

Take CentOS 7 as an example below.

# Install docker
* following the instructions to install docker.
https://docs.docker.com/

# Setup build and test environment
* clone hawq repository
```
git clone https://github.com/apache/incubator-hawq.git .
cd incubator-hawq/contrib/hawq-docker
```
* Get the docker images
```
  make pull (recommended)
OR
  make build
``` 
(Command `make pull` is to pull docker images from Docker Hub, while command `make build` is to build docker images locally. In general, `make pull` is faster than `make build`.)
* setup a 5 nodes virtual cluster for Apache HAWQ build and test.
```
make run
```
Now let's have a look about what we creted.
```
[root@localhost hawq-docker]# docker ps -a
CONTAINER ID        IMAGE                          COMMAND                CREATED             STATUS              PORTS               NAMES
382b2b3360d1        hawq/hawq-test:centos7   "entrypoint.sh bash"   2 minutes ago       Up 2 minutes                            centos7-datanode3
86513c331d45        hawq/hawq-test:centos7   "entrypoint.sh bash"   2 minutes ago       Up 2 minutes                            centos7-datanode2
c0ab10e46e4a        hawq/hawq-test:centos7   "entrypoint.sh bash"   2 minutes ago       Up 2 minutes                            centos7-datanode1
e27beea63953        hawq/hawq-test:centos7   "entrypoint.sh bash"   2 minutes ago       Up 2 minutes                            centos7-namenode
1f986959bd04        hawq/hawq-dev:centos7    "/bin/true"            2 minutes ago       Created                                 centos7-data
```
**centos7-data** is a data container and mounted to /data directory on all other containers to provide a shared storage for the cluster. 

# Build and Test Apache HAWQ
* attach to namenode
```
docker exec -it centos7-namenode bash
```
* check if HDFS working well
```
sudo -u hdfs hdfs dfsadmin -report
```
* clone Apache HAWQ code to /data direcotry
```
git clone https://github.com/apache/incubator-hawq.git /data/hawq
```
* build Apache HAWQ
```
cd /data/hawq
./configure --prefix=/data/hawq-dev
make
make install
```
(When you are using CentOS 6, run command `scl enable devtoolset-2 bash` before
configuring hawq and run command `exit` after installing hawq.) 
* modify Apache HAWQ configuration
```
sed 's|localhost|centos7-namenode|g' -i /data/hawq-dev/etc/hawq-site.xml
echo 'centos7-datanode1' >  /data/hawq-dev/etc/slaves
echo 'centos7-datanode2' >>  /data/hawq-dev/etc/slaves
echo 'centos7-datanode3' >>  /data/hawq-dev/etc/slaves
```
* Initialize Apache HAWQ cluster
```
sudo -u hdfs hdfs dfs -chown gpadmin /
source /data/hawq-dev/greenplum_path.sh
hawq init cluster
```
Now you can connect to database with `psql` command.
```
[gpadmin@centos7-namenode data]$ psql -d postgres
psql (8.2.15)
Type "help" for help.

postgres=# 
```
# More command with this script
```
 Usage:
    To setup a build and test environment:         make run
    To start all containers:                       make start
    To stop all containers:                        make stop
    To remove hdfs containers:                     make clean
    To remove all containers:                      make distclean
    To build images locally:                       make build
    To pull latest images:                         make pull
```


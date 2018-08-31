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
git clone https://github.com/apache/hawq.git .
cd hawq/contrib/hawq-docker
```
* Build the docker images
```
  make build
``` 
(Command `make build` is to build docker images locally.)
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
git clone https://github.com/apache/hawq.git /data/hawq
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
# Store docker images in local docker registry

After your hawq environment is up and running, you could draft a local docker registry to store your hawq images locally for further usage.
* pull and run a docker registry
```
docker pull registry
docker run -d -p 127.0.0.1:5000:5000 registry
```
Make sure you could get the following output
```
curl http://localhost:5000/v2/_catalog
{"repositories":[]}
```
You could push your local hawq images to local repository, let us use "centos7" as example
```
docker tag  hawq/hawq-test:centos7  localhost:5000/hawq-test:centos7
docker tag  hawq/hawq-dev:centos7  localhost:5000/hawq-dev:centos7
docker push localhost:5000/hawq-test
docker push localhost:5000/hawq-dev
```
Now the local registry has images in it
```
curl http://localhost:5000/v2/_catalog
{"repositories":["hawq-dev","hawq-test"]}
```

If we want to pull the images from local repo
```
make pull
``` 


# More command with this script
```
 Usage:
    To setup a build and test environment:         make run
    To start all containers:                       make start
    To stop all containers:                        make stop
    To remove hdfs containers:                     make clean
    To remove all containers:                      make distclean
    To build images locally:                       make build-image
    To pull latest images:                         make pull
    To build Hawq runtime:                         make build-hawq
    To initialize Hawq on Namenode:                make init-hawq
    To start Hawq on Namenode:                     make start-hawq
    To stop Hawq on Namenode:                      make stop-hawq
    To check Hawq status on Namenode:              make status-hawq
    To build PXF runtime:                          make build-pxf
    To initialize PXF on Namenode/Datanodes:       make init-pxf
    To start PXF on Namenode/Datanodes:            make start-pxf
    To stop PXF on on Namenode/Datanodes:          make stop-hawq
    To check PXF status on Namenode/Datanodes:     make status-hawq
```

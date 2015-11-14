# Build Instructions for Apache HAWQ
------

## Requirement
### Operating System

        Linux (tested on redhat 6.x).
### Compiler & Dependencies
To build Apache HAWQ, gcc and some dependencies are needed. The libraries are tested on the given versions. Most of the dependencies can be installed through yum. Other dependencies must be installed through the source tarball.Typically you can use "./configure && make && make install" to install from source tarball.

Libraries that must be installed using source tarball.

        name        version     url
        json-c      0.9         http://oss.metaparadigm.com/json-c/json-c-0.9.tar.gz
        boost       1.56        http://sourceforge.net/projects/boost/files/boost/1.56.0/boost_1_56_0.tar.bz2
        thrift      0.9.1-1     http://archive.apache.org/dist/thrift/0.9.1/thrift-0.9.1.tar.gz
        protobuf    2.5.0       https://github.com/google/protobuf/tree/v2.5.0
        curl        7.44.0      http://www.curl.haxx.se/download/curl-7.44.0.tar.gz
        libhdfs3    Github      https://github.com/PivotalRD/libhdfs3.git
        libyarn                 Code shipped with Apache HAWQ (under incubator-hawq/depends/)
                                Steps:
                                    cd depends/libyarn/
                                    mkdir build
                                    cd build
                                    ../bootstrap --prefix=/usr/local/
                                    make
                                    sudo make install


Libraries that can be installed through yum. 

        name                version
        epel-release        6-8
        make                3.81
        gcc                 4.4.7
        gcc-c++             4.4.7
        gperf               3.0.4
        snappy-devel        1.1.3
        bzip2-devel         1.0.6
        python-devel        2.6.2
        libevent-devel      1.4.6
        krb5-devel          1.11.3
        libuuid-devel       2.26.2
        libgsasl-devel      1.8.0
        libxml2-devel       2.7.8
        zlib-devel          1.2.3
        readline-devel      6
        openssl-devel       0.9.8
        bison-devel         2.5
        apr-devel           1.2.12
        libyaml-devel       0.1.1
        flex-devel          2.5.35
## Build
After the libraries list above are installed, get source code:

        git clone https://github.com/apache/incubator-hawq.git
The code directory is CODEHOME/incubator-hawq. Then cd CODEHOME/incubator-hawq and build Apache HAWQ under this directory.

Run command to generate makefile.

        ./configure
Or you can use --prefix=/hawq/install/path to change the Apache HAWQ install path.

        ./configure --prefix=/hawq/install/path
Run the command below for more configuration.

        ./configure --help
Run command to build.

        make
To build concurrently, run make with -j option.

        make -j8
## Install
To install HAWQ, run command

        make install
## Install YARN (Optional)
If you want to integrate with YARN for resource management, you need to install YARN first.
## Test
### Unit Test ###
To do unit test, go to the src/backend and run unittest.

        cd src/backend
        make unittest-check
### Installcheck-good Test ###
After installing HAWQ , please ensure HDFS work before initializing HAWQ.

        source /install/dir/greenplum_path.sh
        hawq init cluster
        make installcheck-good

## Wiki
https://cwiki.apache.org/confluence/display/HAWQ/Apache+HAWQ+Home

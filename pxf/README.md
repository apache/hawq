The PXF extensions library for HAWQ
===================================

Table of Contents
=================

* Introduction
* Package Contents
* Building

Introduction
============

PXF is an extensible framework that allows HAWQ to query external data files, whose metadata is not managed by HAWQ.
PXF includes built-in connectors for accessing data that exists inside HDFS files, Hive tables, HBase tables and more.
Users can also create their own connectors to other data storages or processing engines.
To create these connectors using JAVA plugins, see the PXF API and Reference Guide online.

Package Contents
================

PXF is distributed as a set of RPMs -

    pxf/
        └── rpm
            ├── pxf-service-$version.noarch.rpm
            ├── pxf-hdfs-$version.noarch.rpm
            ├── pxf-hive-$version.noarch.rpm
            ├── pxf-hbase-$version.noarch.rpm
            └── pxf-json-$version.noarch.rpm

Building
========

    ./gradlew clean build [buildRpm] [distTar]

    For all available tasks run: ./gradlew tasks
    

Building for a specific database
================================

PXF could be built for a diffent databases, currently HAWQ and Greenplum are supported.
Configuration for target databases are stored in **gradle/profiles**.
HAWQ is a default database. To build it for Greenplum:

    ./gradlew clean build [buildRpm] [distTar] -Ddatabase="gpdb"
    

Profiling
=========

If you wish to profile the PXF, you could use [Java profiler](https://www.ej-technologies.com/products/jprofiler/overview.html). You can download JProfiler agent and update pxf-env.sh to set port say 10001 for profiler access.

    export PXF_JVM_OPTS="-Xmx2g -Xms1g -agentpath:jprofiler10/bin/linux-x64/libjprofilerti.so=port=10001"
    




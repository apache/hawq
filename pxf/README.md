The PXF extensions library for HAWQ
===================================

Copyright © 2014 Pivotal Software, Inc. All Rights reserved.

Table of Contents
=================

* Introduction
* Package Contents
* Building

Introduction
============

PXF is an extensible framework that allows HAWQ to query external system data.
PXF includes built-in connectors for accessing data that exists inside HDFS files, Hive tables, HBase tables and more.
Users can also create their own connectors to other parallel data stores or processing engines.
To create these connectors using JAVA plugins, see the Pivotal Extension Framework API and Reference Guide .

Package Contents
================

PXF is distributed as a set of RPMs -

    pxf/
        └── rpm
            ├── pxf-service-$version.noarch.rpm
            ├── pxf-hdfs-$version.noarch.rpm
            ├── pxf-hive-$version.noarch.rpm
            └── pxf-hbase-$version.noarch.rpm


Building
========

    ./gradlew clean build [buildRpm] [distTar]

    For all available tasks run: ./gradlew tasks


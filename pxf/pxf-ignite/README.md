# Accessing Ignite database using PXF

The PXF Ignite plug-in allows to read data from Ignite database.


# Prerequisites

Check the following before using the PXF Ignite plug-in:
* The Ignite client is installed on the machine used as `IGNITE_HOST`, and it accepts REST queries from every PXF node;
* The Ignite plug-in is installed on all PXF nodes;
* `pxf-public.classpath` and `pxf-profiles.xml` located in `/etc/pxf/conf` support the Ignite plug-in. Note that `pxf-service` must be restarted if these files were changed;


# Syntax

```
CREATE [READABLE] EXTERNAL TABLE table_name (
    column_name data_type [, ...] | LIKE other_table 
)
LOCATION ('pxf://namenode[:port]?PROFILE=Ignite[&<extra-parameter>&<extra-parameter>&...]')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');
```
where each `<extra-parameter>` is one of the following:
* `IGNITE_HOST=<ignite_host_address_with_port>`. The location of Ignite client node. If not given, `127.0.0.1:8080` is used by default;
* `IGNITE_CACHE=<ignite_cache_name>`. The name of Ignite cache to use. If not given, this parameter is not included in queries from PXF to Ignite, thus the Ignite defaults will be used (at the moment, Ignite uses `Default` cache in this case). **Note** that this option is case-sensitive;
* `BUFFER_SIZE=<int >= 1>`. The number of tuples stored in cache used by PXF; the same number of tuples is send by Ignite per a response. If not given, `128` is used by default;
* `PARTITION_BY=<column>:<column_type>`. See below;
* `RANGE=<start_value>:<end_value>`. See below;
* `INTERVAL=<value>[:<unit>]`. See below.


# Jdbc Table Fragments
## Introduction

PXF Ignite plugin supports simultaneous access to Ignite database from multiple PXF segments. *Partitioning* should be used in order to perform such operation.

Partitioning in PXF Ignite plug-in works just like in PXF JDBC plug-in.

Partitioning is optional. However, a bug in the `pxf-service` which makes partitioning a required parameter was fixed only in [this commit](https://github.com/apache/incubator-hawq/commit/0d620e431026834dd70c9e0d63edf8bb28b38227) (17th Jan 2018), so the older versions of PXF may return an exception if a query does not contain a meaningful `PARTITION_BY` parameter.


## Syntax

To use partitions, add a set of `<ignite-parameter>`:
```
&PARTITION_BY=<column>:<column_type>&RANGE=<start_value>:<end_value>[&INTERVAL=<value>[:<unit>]]
```

* The `PARTITION_BY` parameter indicates which column to use as the partition column. Only one column can be used as a partition column.
    * The `<column>` is the name of a partition column;
    * The `<column_type>` is the datatype of a partition column. At the moment, the **supported types** are `INT`, `DATE` and `ENUM`. The `DATE` format is `yyyy-MM-dd`.

* The `RANGE` parameter indicates the range of data to be queried. It is left-closed, thus it produces ranges like:
    * `[ <start_value> ; <end_value> )`, 
    * `... >= start_value AND ... < end_value`;

* The `INTERVAL` parameter is **required** for `INT` and `DATE` partitions. This parameter is ignored if `<column_type>` is `ENUM`.
    * The `<value>` is the size of each fragment (the last one may be smaller). Note that by default PXF does not support more than 100 fragments;
    * The `<unit>` **must** be provided if `<column_type>` is `DATE`. At the moment, only `year`, `month` and `day` are supported. This parameter is ignored in case of any other `<column_type>`.

Example partitions:
* `&PARTITION_BY=id:int&RANGE=42:142&INTERVAL=1`
* `&PARTITION_BY=createdate:date&RANGE=2008-01-01:2010-01-01&INTERVAL=1:month`
* `&PARTITION_BY=grade:enum&RANGE=excellent:good:general:bad`



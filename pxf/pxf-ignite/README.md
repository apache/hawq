# Accessing Ignite database using PXF

The PXF Ignite plug-in enables to access the [Ignite database](https://ignite.apache.org/) (both read and write operations are supported) via REST API.


# Prerequisites

Check the following before using the plug-in:

* The Ignite plug-in is installed on all PXF nodes;

* The Ignite client is installed and running at the `IGNITE_HOST` (`localhost` by default; see below), and it accepts http queries from the PXF (note that *enabling Ignite REST API does not require changes in Ignite configuration*; see the instruction on how to do that at https://apacheignite.readme.io/docs/rest-api#section-getting-started).


# Syntax

```
CREATE [READABLE] EXTERNAL TABLE <table_name> (
    <column_name> <data_type>[, <column_name> <data_type>, ...] | LIKE <other_table>
)
LOCATION ('pxf://<ignite_table_name>?PROFILE=Ignite[&<extra-parameter>&<extra-parameter>&...]')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');
```
where each `<extra-parameter>` is one of the following:
* `IGNITE_HOST=<ignite_host_address_with_port>`. The location of Ignite client node. If not given, `127.0.0.1:8080` is used by default;
* `IGNITE_CACHE=<ignite_cache_name>`. The name of Ignite cache to use. If not given, this parameter is not included in queries from PXF to Ignite, thus Ignite default values will be used (at the moment, this is `Default` cache). This option is **case-sensitive**;
* `BUFFER_SIZE=<unsigned_int>`. The number of tuples send to (from) Ignite per a response. The same number of tuples is stored in in-plug-in cache. The values `0` and `1` are equal (cache is not used, each tuple is passed in it's own query to Ignite). If not given, `128` is used by default;
* `PARTITION_BY=<column>:<column_type>`. See below;
* `RANGE=<start_value>:<end_value>`. See below;
* `INTERVAL=<value>[:<unit>]`. See below.


# Partitioning
## Introduction

PXF Ignite plugin supports simultaneous access to Ignite database from multiple PXF segments. *Partitioning* should be used in order to perform such operation.

Partitioning in PXF Ignite plug-in works just like in PXF JDBC plug-in.

This feature is optional. However, a bug in the `pxf-service` which makes partitioning necessary for any query was fixed only on 17th Jan 2018 in [this commit](https://github.com/apache/incubator-hawq/commit/0d620e431026834dd70c9e0d63edf8bb28b38227), so the older versions of PXF may return an exception if a query does not contain a meaningful `PARTITION_BY` parameter.


## Syntax

To use partitions, add a set of `<ignite-parameter>`s:
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

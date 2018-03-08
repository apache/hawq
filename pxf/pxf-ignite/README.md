# Accessing Ignite database using PXF

The PXF Ignite plug-in enables to access the [Apache Ignite database](https://ignite.apache.org/) (both read and write operations are supported) via REST API.


## Prerequisites

Check the following before using the plug-in:

* The Ignite plug-in is installed on all PXF nodes;

* The Apache Ignite client is installed and running at the `IGNITE_HOST` (`localhost` by default; this can be changed, see syntax below), and it accepts http queries from the PXF (note that *enabling Ignite REST API does not require changes in Ignite configuration*; see the instruction on how to do that at https://apacheignite.readme.io/docs/rest-api#section-getting-started).


## Syntax

```
CREATE [READABLE | WRITABLE] EXTERNAL TABLE <table_name> (
    <column_name> <data_type>[, <column_name> <data_type>, ...] | LIKE <other_table>
)
LOCATION ('pxf://<ignite_table_name>?PROFILE=Ignite[&<extra-parameter>&<extra-parameter>&...]')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');
```
where each `<extra-parameter>` is one of the following:
* `IGNITE_HOST=<ignite_host_address_with_port>`. The location of Ignite client node. If not given, `127.0.0.1:8080` is used by default;
* `IGNITE_CACHE=<ignite_cache_name>`. The name of Ignite cache to use. If not given, this parameter is not included in queries from PXF to Ignite, thus Ignite default values will be used (at the moment, this is `Default` cache). This option is **case-sensitive**;
* `BUFFER_SIZE=<unsigned_int>`. The number of tuples send to (from) Ignite per a response. The same number of tuples is stored in in-plug-in cache. The values `0` and `1` are equal (cache is not used, each tuple is passed in its own query to Ignite). If not given, `128` is used by default;
* `PARTITION_BY=<column>:<column_type>`. See below;
* `RANGE=<start_value>:<end_value>`. See below;
* `INTERVAL=<value>[:<unit>]`. See below.


## Write access

PXF Ignite plugin supports `INSERT` queries. However, due to the usage of REST API, this function has a technical limit: URL can not be longer than approx. 2000 characters. This makes the `INSERT` of very long tuples of data impossible.

Due to this limitation, the recommended value of `BUFFER_SIZE` for `INSERT` queries is `1`. Note that this slightly decreases the perfomance.


## Partitioning
### Introduction

PXF Ignite plugin supports simultaneous **read** access to the Apache Ignite database from multiple PXF segments. *Partitioning* should be used in order to perform such operation.

This feature is optional. If partitioning is not used, all the data will be retrieved by a single PXF segment.


### Mechanism

Partitioning in PXF Ignite plug-in works just like in PXF JDBC plug-in.

If partitioning is activated (a valid set of the required parameters is present in the `EXTERNAL TABLE` description; see syntax below), the SELECT query is split into a set of small queries, each of which is called a *fragment*. All the fragments are processed by separate PXF instances simultaneously. If there are more fragments than PXF instances, some instances will process more than one fragment; if only one PXF instance is available, it will process all the fragments.

Extra constraints (`WHERE` expressions) are automatically added to each fragment to guarantee that every tuple of data is retrieved from the Apache Ignite database exactly once.


### Syntax

To use partitions, add a set of `<ignite-parameter>`s:
```
&PARTITION_BY=<column>:<column_type>&RANGE=<start_value>:<end_value>[&INTERVAL=<value>[:<unit>]]
```

* The `PARTITION_BY` parameter indicates which column to use as the partition column. Only one column can be used as a partition column.
    * The `<column>` is the name of a partition column;
    * The `<column_type>` is the datatype of a partition column. At the moment, the **supported types** are `INT`, `DATE` and `ENUM`. The `DATE` format is `yyyy-MM-dd`.

* The `RANGE` parameter indicates the range of data to be queried. If the partition type is `ENUM`, the `RANGE` parameter must be a list of values, each of which forms its own fragment. In case of `INT` and `DATE` partitions, this parameter must be a finite left-closed range ("infinity" values are not supported):
    * `[ <start_value> ; <end_value> )`
    * `... >= start_value AND ... < end_value`;

* The `INTERVAL` parameter is **required** for `INT` and `DATE` partitions. This parameter is ignored if `<column_type>` is `ENUM`.
    * The `<value>` is the size of each fragment (the last one may be smaller). Note that by default PXF does not support more than 100 fragments;
    * The `<unit>` **must** be provided if `<column_type>` is `DATE`. At the moment, only `year`, `month` and `day` are supported. This parameter is ignored in case of any other `<column_type>`.

Example partitions:
* `&PARTITION_BY=id:int&RANGE=42:142&INTERVAL=2`
* `&PARTITION_BY=createdate:date&RANGE=2008-01-01:2010-01-01&INTERVAL=1:month`
* `&PARTITION_BY=grade:enum&RANGE=excellent:good:general:bad`

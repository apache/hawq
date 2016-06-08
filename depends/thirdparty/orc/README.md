# [Apache ORC](https://orc.apache.org/)

ORC is a self-describing type-aware columnar file format designed for
Hadoop workloads. It is optimized for large streaming reads, but with
integrated support for finding required rows quickly. Storing data in
a columnar format lets the reader read, decompress, and process only
the values that are required for the current query. Because ORC files
are type-aware, the writer chooses the most appropriate encoding for
the type and builds an internal index as the file is written.
Predicate pushdown uses those indexes to determine which stripes in a
file need to be read for a particular query and the row indexes can
narrow the search to a particular set of 10,000 rows. ORC supports the
complete set of types in Hive, including the complex types: structs,
lists, maps, and unions.

## ORC File Library

This project includes both a Java library for reading and writing and
a C++ library for reading the _Optimized Row Columnar_ (ORC) file
format. The C++ and Java libraries are completely independent of each
other and will each read all versions of ORC files.

### Building

* Install java 1.7 or higher
* Install maven 3 or higher
* Install cmake

To build a release version with debug information:
```shell
% mkdir build
% cd build
% cmake ..
% make package
% make test-out

```

To build a debug version:
```shell
% mkdir build
% cd build
% cmake .. -DCMAKE_BUILD_TYPE=DEBUG
% make package
% make test-out

```

To build a release version without debug information:
```shell
% mkdir build
% cd build
% cmake .. -DCMAKE_BUILD_TYPE=RELEASE
% make package
% make test-out

```

To build only the Java library:
```shell
% cd java
% mvn package

```

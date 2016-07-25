![HAWQ](http://hawq.incubator.apache.org/images/logo-hawq.png) [![https://travis-ci.org/apache/incubator-hawq.png](https://travis-ci.org/apache/incubator-hawq.png)](https://travis-ci.org/apache/incubator-hawq)

[Website](http://hawq.incubator.apache.org/) |
[Wiki](https://cwiki.apache.org/confluence/display/HAWQ/Apache+HAWQ+Home) |
[Documentation](http://hdb.docs.pivotal.io/) |
[Developer Mailing List](mailto:dev@hawq.incubator.apache.org) |
[User Mailing List](mailto:user@hawq.incubator.apache.org) |
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

# Build & Install & Test
Please refer to the [BUILD_INSTRUCTIONS][1] file.

  [1]: https://github.com/apache/incubator-hawq/blob/master/BUILD_INSTRUCTIONS.md "BUILD_INSTRUCTIONS"

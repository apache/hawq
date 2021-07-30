# Accessing Jdbc Table Data

The PXF JDBC plug-in reads data stored in Traditional relational database,ie : mysql,ORACLE,postgresql.

PXF-JDBC plug-in is the client of the database, the host running the database engine does not need to
deploy PXF.


# Prerequisites

Check the following before using PXF to access JDBC Table:
* The PXF JDBC plug-in is installed on all cluster nodes.
* The JDBC JAR files are installed on all cluster nodes, and added to file - 'pxf-public.classpath'
* You have tested PXF on HDFS.

# Using PXF Tables to Query JDBC Table
Jdbc tables are defined in same schema in PXF.The PXF table has the same column name
as Jdbc Table, and the column type requires a mapping of Jdbc-HAWQ.

## Syntax Example
The following PXF table definition is valid for Jdbc Table.

    CREATE [READABLE|WRITABLE] EXTERNAL TABLE table_name
        ( column_name data_type [, ...] | LIKE other_table )
    LOCATION ('pxf://namenode[:port]/jdbc-schema-name.jdbc-table-name?<pxf-parameters><&custom-parameters>')
    FORMAT 'CUSTOM' (formatter='pxfwritable_import')
If `jdbc-schema-name` is omitted, pxf will default to the `default` schema.

The `column_name` must exists in jdbc-table,`data_type` equals or similar to
the jdbc-column type.

where `<pxf-parameters>` is:

    [FRAGMENTER=org.apache.hawq.pxf.plugins.jdbc.JdbcPartitionFragmenter
    &ACCESSOR=org.apache.hawq.pxf.plugins.jdbc.JdbcReadAccessor
    &RESOLVER=org.apache.hawq.pxf.plugins.jdbc.JdbcReadResolver]
    | PROFILE=Jdbc

where `<custom-parameters>` is:

    JDBC_DRIVER=<jdbc-driver-class-name>
     &DB_URL=<jdbc-url>&USER=<database-user>&PASS=<password>

## Jdbc Table to HAWQ Data Type Mapping
Jdbc-table and hawq-table data type system is similar to, does not require
a special type of mapping.
# Usage
The following to mysql, for example, describes the use of PDF-JDBC.

To query MySQL Table in HAWQ, perform the following steps:
1. create Table in MySQL

         mysql> use demodb;
         mysql> create table myclass(
                 id int(4) not null primary key,
                 name varchar(20) not null,
                 gender int(4) not null default '0',
                 degree double(16,2));`
2. insert test data

        insert into myclass values(1,"tom",1,90);
        insert into myclass values(2,'john',0,94);
        insert into myclass values(3,'simon',1,79);
3. copy mysql-jdbc jar files to `/usr/lib/pxf` (on all cluster nodes), and
edit `/etc/pxf/conf/pxf-public.classpath` , add :

        /usr/lib/pxf/mysql-connector-java-*.jar

     Restart all pxf-engine.

4. create Table in HAWQ:

        gpadmin=# CREATE EXTERNAL TABLE myclass(id integer,
             name text,
             gender integer,
             degree float8)
             LOCATION ('pxf://localhost:51200/demodb.myclass'
                     '?PROFILE=JDBC'
                     '&JDBC_DRIVER=com.mysql.jdbc.Driver'
                     '&DB_URL=jdbc:mysql://192.168.200.6:3306/demodb&USER=root&PASS=root'
                     )
            FORMAT 'CUSTOM' (Formatter='pxfwritable_import');

MySQL instance IP: 192.168.200.6, port: 3306.

5. query mysql data in HAWQ:

        gpadmin=# select * from myclass;
        gpadmin=# select * from myclass where id=2;

# Jdbc Table Fragments
## intro
PXF-JDBC plug-in as a  client to access jdbc database.By default, there is
only one pxf-instance connectied JDBC Table.If the jdbc table data is large,
you can also use multiple pxf-instance to access the JDBC table by fragments.

## Syntax
where `<custom-parameters>` can use following partition parameters:

    PARTITION_BY=column_name:column_type&RANGE=start_value[:end_value]&INTERVAL=interval_num[:interval_unit]
The `PARTITION_BY` parameter indicates which  column to use as the partition column.
It can be split by colon(':'),the `column_type` current supported : `date|int|enum` .
The Date format is `yyyy-MM-dd`.
The `PARTITION_BY` parameter can be null, and there will be only one fragment.

The `RANGE` parameter indicates the range of data to be queried , it can be split by colon(':').
 The range is left-closed, ie: `>= start_value AND < end_value` .
 If the `column_type` is `int`, the `end_value` can be empty.
 If the `column_type` is `enum`,the parameter `RANGE` can be empty.

The `INTERVAL` parameter can be split by colon(':'), indicate the interval
 value of one fragment. When `column_type` is `date`,this parameter must
 be split by colon, and `interval_unit` can be `year|month|day`. When
 `column_type` is int, the `interval_unit` can be empty. When `column_type`
 is enum,the `INTERVAL` parameter can be empty.

The syntax examples is :

    * PARTITION_BY=createdate:date&RANGE=2008-01-01:2010-01-01&INTERVAL=1:month'
    * PARTITION_BY=year:int&RANGE=2008:2010&INTERVAL=1
    * PARTITION_BY=grade:enum&RANGE=excellent:good:general:bad

## Usage
MySQL Table:

    CREATE TABLE sales (id int primary key, cdate date, amt decimal(10,2),grade varchar(30))
HAWQ Table:

    CREATE EXTERNAL TABLE sales(id integer,
                 cdate date,
                 amt float8,
                 grade text)
                 LOCATION ('pxf://localhost:51200/sales'
                         '?PROFILE=JDBC'
                         '&JDBC_DRIVER=com.mysql.jdbc.Driver'
                         '&DB_URL=jdbc:mysql://192.168.200.6:3306/demodb&USER=root&PASS=root'
                         '&PARTITION_BY=cdate:date&RANGE=2008-01-01:2010-01-01&INTERVAL=1:year'
                         )
                 FORMAT 'CUSTOM' (Formatter='pxfwritable_import');
At PXF-JDBC plugin,this will generate 2 fragments.Then HAWQ assign these fragments to 2 PXF-instance
to access jdbc table data.
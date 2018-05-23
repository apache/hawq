# PXF extension to Greenplum to provide access to Parquet formatted data stored in S3

<div align="center">
<img src="./images/Greenplum_Logo.png" align="middle" alt="Greenplum" style="max-width: 20%;"/>
<img src="./images/Parquet_Logo.png" align="middle" alt="Parquet" style="max-width: 20%;"/>
<img src="./images/S3_Logo.png" align="middle" alt="S3" style="max-width: 20%;"/>
</div>

## TODO
* Add support for Avro data

## Collect all JARs and install on Greenplum segment server(s)
* The Gradle build will deposit all the transitive dependencies into `./build/libs`
* `tar -C ./pxf-s3-parquet/build/libs -cvf pxf-s3-jars.tar .`
* Copy this up to the Greenplum server(s) and extract into, say, `~/gpadmin/pxf-jars`
* Add this list of JARS to the `$GPHOME/pxf/conf/pxf-public.classpath` file:
  ```
  ls -1 ~/pxf-jars/* >> $GPHOME/pxf/conf/pxf-public.classpath
  ```

## Add new PXF profile
* Edit `$GPHOME/pxf/conf/pxf-profiles.xml`:
```
<profiles>
  <profile>
    <name>S3ParquetWrite</name>
    <description>A profile for writing Parquet data to S3</description>
    <plugins>
      <fragmenter>org.apache.hawq.pxf.plugins.s3.S3Fragmenter</fragmenter>
      <accessor>org.apache.hawq.pxf.plugins.s3.S3ParquetWriteAccessor</accessor>
      <resolver>org.apache.hawq.pxf.plugins.s3.S3ParquetWriteResolver</resolver>
    </plugins>
  </profile>
  <profile>
    <name>S3ParquetRead</name>
    <description>A profile for reading Parquet data from S3; does not support nested data</description>
    <plugins>
      <fragmenter>org.apache.hawq.pxf.plugins.s3.S3Fragmenter</fragmenter>
      <accessor>org.apache.hawq.pxf.plugins.s3.S3ParquetAccessor</accessor>
      <resolver>org.apache.hawq.pxf.plugins.s3.S3ParquetResolver</resolver>
    </plugins>
  </profile>
  <profile>
    <name>S3ParquetJsonRead</name>
    <description>A profile for reading Parquet data from S3, as JSON; useful with nested data</description>
    <plugins>
      <fragmenter>org.apache.hawq.pxf.plugins.s3.S3Fragmenter</fragmenter>
      <accessor>org.apache.hawq.pxf.plugins.s3.S3ParquetJsonAccessor</accessor>
      <resolver>org.apache.hawq.pxf.plugins.s3.S3ParquetJsonResolver</resolver>
    </plugins>
  </profile>
</profiles>
```

## Restart the PXF server on all segment hosts
```
  $GPHOME/pxf/bin/pxf restart
```

## Example: reading data from Parquet files in S3
* Data source: you can upload into S3 the five Parquet files found [here](https://github.com/Teradata/kylo/tree/master/samples/sample-data/parquet)
* DDL: replace the `S3_ACCESS_KEY`, `S3_SECRET_KEY` and `S3_REGION` values
```
-- Parquet input, JSON output
DROP EXTERNAL TABLE IF EXISTS parquet_json;
CREATE EXTERNAL TABLE parquet_json (js JSON)
LOCATION ('pxf://pxf-s3-devel/the-data/userdata?S3_REGION=us-east-1&S3_ACCESS_KEY=ACFOCBFFDJFHZ69M7GM7&S3_SECRET_KEY=3DFq0kV01aEdNzr5uxkeN7Hr/cZv6erDjM3z4NsB&PROFILE=S3ParquetJsonRead')
FORMAT 'TEXT' (DELIMITER 'OFF');
```
* Queries on this data use the Greenplum JSON operators described [here](https://gpdb.docs.pivotal.io/500/admin_guide/query/topics/json-data.html#topic_gn4_x3w_mq)
* One possible query for this data set.  Note the use of parenthesis around the `js->>'salary'`, since the cast operator (`::`) binds more tightly than the `->>` operator.
```
SELECT js->>'country', AVG((js->>'salary')::float)::NUMERIC(9, 2)
FROM parquet_json
GROUP BY 1
ORDER BY 2 ASC
LIMIT 10;
```

## Example: write data into Parquet files in S3
* DDL: replace the `S3_ACCESS_KEY`, `S3_SECRET_KEY` and `S3_REGION` values
```
DROP EXTERNAL TABLE IF EXISTS write_parquet;
CREATE WRITABLE EXTERNAL TABLE write_parquet (a INT, b TEXT, c BOOLEAN)
LOCATION ('pxf://pxf-s3-devel/test-write?S3_REGION=us-east-1&S3_ACCESS_KEY=ACFOCBFFDJFHZ69M7GM7&S3_SECRET_KEY=3DFq0kV01aEdNzr5uxkeN7Hr/cZv6erDjM3z4NsB&PROFILE=S3ParquetWrite')
FORMAT 'CUSTOM' (formatter='pxfwritable_export');
```
* Insert some rows into this table
```
INSERT INTO write_parquet
(b, a, c) VALUES ('First value for b', 1001, false), ('Second value for b', 1002, true);
```
* Build a *readable* Parquet S3 table, to verify what was written (JSON format)
```
DROP EXTERNAL TABLE IF EXISTS read_what_we_wrote;
CREATE EXTERNAL TABLE read_what_we_wrote (js JSON)
LOCATION ('pxf://pxf-s3-devel/test-write?S3_REGION=us-east-1&S3_ACCESS_KEY=ACFOCBFFDJFHZ69M7GM7&S3_SECRET_KEY=3DFq0kV01aEdNzr5uxkeN7Hr/cZv6erDjM3z4NsB&PROFILE=S3ParquetJsonRead')
FORMAT 'TEXT' (DELIMITER 'OFF');
```
* Finally, query that table to verify that values made it
```
SELECT js->>'b' a, (js->>'a')::INT a, (js->>'c')::BOOLEAN c
FROM read_what_we_wrote
ORDER BY 2 ASC;
```
* Or, use a table which uses the primitive data types (doesn't support nested data structures)
```
DROP EXTERNAL TABLE IF EXISTS read_primitives;
CREATE EXTERNAL TABLE read_primitives (LIKE write_parquet)
LOCATION ('pxf://pxf-s3-devel/test-write?S3_REGION=us-east-1&S3_ACCESS_KEY=ACFOCBFFDJFHZ69M7GM7&S3_SECRET_KEY=3DFq0kV01aEdNzr5uxkeN7Hr/cZv6erDjM3z4NsB&PROFILE=S3ParquetRead')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
```
* And read from this table
```
SELECT *
FROM read_primitives
ORDER BY a ASC;
```

## Caveats
* You have to replace a JAR in the standard classpath with one we need due to the S3 libs:
```
[gpadmin@gpdbsne conf]$ diff -u pxf-private.classpath.ORIG pxf-private.classpath
--- pxf-private.classpath.ORIG	2018-04-17 19:17:48.178225024 +0000
+++ pxf-private.classpath	2018-04-17 19:18:35.114744844 +0000
@@ -52,7 +52,7 @@
 /usr/lib/hadoop/client/commons-logging.jar
 /usr/lib/hadoop/client/guava.jar
 /usr/lib/hadoop/client/htrace-core4.jar
-/usr/lib/hadoop/client/jackson-core.jar
+/home/gpadmin/pxf-jars/jackson-core-2.6.7.jar
 /usr/lib/hadoop/client/jackson-mapper-asl.jar
 /usr/lib/hadoop/client/jetty-*.jar
 /usr/lib/hadoop/client/jersey-core.jar
 ```

* Out of memory?
If the query fails, and you find `java.lang.OutOfMemoryError: GC overhead limit exceeded` in the catalina.out,
try increasing the heap size for the Java PXF process (it's running Tomcat).
  - File: `/usr/local/greenplum-db/pxf/pxf-service/bin/setenv.sh` on each segment host
  - Line to change: `JVM_OPTS="-Xmx16g -Xms8g -Xss256K -Dpxf.log.dir=/usr/local/greenplum-db/pxf/logs "`
  - The `-Xmx16g -Xms8g` is the *new* value (it was set to a much lower value initially)

* Inconsistency in handling TIMESTAMP and DATE types, with Parquet / Avro
The `BridgeOutputBuilder` class contains the following check, which makes it impossible to serialize
these two types, using the Avro schema approach typical with Parquet, in the way that they are usually
handled (e.g. `DATE` would be represented at `int`, and `TIMESTAMP` as `long`):
```
  for (int i = 0; i < size; i++) {
      OneField current = recFields.get(i);
      if (!isTypeInSchema(current.type, schema[i])) {
          throw new BadRecordException("For field " + colNames[i]
                  + " schema requires type "
                  + DataType.get(schema[i]).toString()
                  + " but input record has type "
                  + DataType.get(current.type).toString());
      }

      fillOneGPDBWritableField(current, i);
  }
```
That `isTypeInSchema()` method incorporates the following logic, which basically implies that these two
types can be handled, so long at they are serialized within Parquet as `String` type:
```
  boolean isStringType(DataType type) {
    return Arrays.asList(DataType.VARCHAR, DataType.BPCHAR, DataType.TEXT,
      DataType.NUMERIC, DataType.TIMESTAMP, DataType.DATE).contains(type);
  }
```
So, if *we* are writing Parquet data from Greenplum, we would write consistent with this approach, but
it remains to be seen how well we will handle Parquet data written by some other system.

## Notes
* Here's an example of an INSERT into the S3 Parquet external table, reading from an s3 external table
```
gpadmin=# INSERT INTO osm_parquet_write SELECT id, extract(epoch from date_time)::int date_time, uid, lat, lon, name, key_value from osm_ext;
NOTICE:  Found 31 data formatting errors (31 or more input rows). Rejected related input data.
INSERT 0 5327616
Time: 12495.106 ms
```
* Example: read of that Parquet data, using the `S3ParquetJsonRead` profile
```
gpadmin=# select count(*) from osm_parquet_read;
  count
---------
 5327616
(1 row)

Time: 184331.683 ms
```
* Alternatively, read that same data, but from a table created using the `S3ParquetRead` profile
```
gpadmin=# select count(*) from osm_prim_read;                                                                                                                                               count
---------
 5327616
(1 row)

Time: 6549.945 ms
```
So far, it seems that the JSON read mode is quite a bit (about 30x) slower, so there's work to be
done to explore what's causing that.

* A [reference on tuning Parquet](https://www.slideshare.net/RyanBlue3/parquet-performance-tuning-the-missing-guide)
* An [article about moving to Parquet file](https://www.enigma.com/blog/moving-to-parquet-files-as-a-system-of-record)
* [This script](./csv_to_parquet_pyspark.py) was used to verify that this solution is able to read Parquet data files written by Spark


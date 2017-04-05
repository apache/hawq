set session role= 'userpxf5';
CREATE EXTERNAL TABLE test_hbase (recordkey bytea,"f1:col1" int) 
	LOCATION ('pxf://localhost:51200/test_hbase?Profile=HBase')
	FORMAT 'CUSTOM' (Formatter='pxfwritable_import');

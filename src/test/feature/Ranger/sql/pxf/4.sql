set session role= 'userpxf3';
CREATE EXTERNAL TABLE pxf_hdfs_textsimple_r1(location text, month text, num_orders int, total_sales float8)
	LOCATION ('pxf://localhost:51200/ranger_test/pxfwritable_hdfs_textsimple1?PROFILE=HdfsTextSimple')
	FORMAT 'CSV';

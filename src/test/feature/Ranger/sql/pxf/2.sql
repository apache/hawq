set session role= 'userpxf2';
CREATE WRITABLE EXTERNAL TABLE pxf_hdfs_writabletbl_1(location text, month text, num_orders int, total_sales float8)
	LOCATION ('pxf://localhost:51200/ranger_test/pxfwritable_hdfs_textsimple1?PROFILE=HdfsTextSimple')
	FORMAT 'TEXT' (delimiter=E',');

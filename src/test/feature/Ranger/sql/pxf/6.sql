set session role= 'userpxf4';
CREATE EXTERNAL TABLE testhive_ext(a int, b int)
	LOCATION ('pxf://localhost:51200/default.testhive_ext?PROFILE=Hive')
	FORMAT 'custom' (formatter='pxfwritable_import');

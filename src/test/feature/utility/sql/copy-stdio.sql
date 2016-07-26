create temp table copytest3 (
	c1 int, 
	"col with , comma" text, 
	"col with "" quote"  int) distributed by (c1);

copy copytest3 from stdin csv header;
this is just a line full of junk that would error out if parsed
1,a,1
2,b,2
\.

copy copytest3 to stdout csv header;
-- copy with error table
CREATE TABLE number (a INT) DISTRIBUTED BY (a);

COPY number FROM STDIN LOG ERRORS INTO err_copy SEGMENT REJECT LIMIT 10 ROWS;
these are invalid line should be insert into error table.
a
b
c
d
e
f
g
h
\.

select relname,filename,linenum,bytenum,errmsg,rawdata,rawbytes from err_copy order by linenum;
select * from number; --should be empty
\d err_copy

DROP TABLE err_copy;

COPY number FROM STDIN LOG ERRORS INTO err_copy SEGMENT REJECT LIMIT 10 ROWS;
these are invalid line should be insert into error table.
a
1
b
2
c
3
d
4
e
5
f
6
g
7
h
\.

select relname,filename,linenum,bytenum,errmsg,rawdata,rawbytes from err_copy order by linenum;
select count(*) from number; --should be 7
DROP TABLE err_copy;

TRUNCATE number;

COPY number FROM STDIN LOG ERRORS INTO err_copy SEGMENT REJECT LIMIT 10 ROWS;
these are invalid line should be insert into error table.
a
1
b
2
c
3
d
4
e
5
f
6
g
7
h
i
\.

select relname,filename,linenum,bytenum,errmsg,rawdata,rawbytes from err_copy order by linenum; -- should not exist
select count(*) from number; --should be empty

TRUNCATE number;
CREATE TABLE err_copy (cmdtime timestamp with time zone, relname text, filename text, linenum integer, bytenum integer, errmsg text, rawdata text, rawbytes bytea) distributed randomly;

COPY number FROM STDIN LOG ERRORS INTO err_copy SEGMENT REJECT LIMIT 10 ROWS;
these are invalid line should be insert into error table.
a
1
b
2
c
3
d
4
e
5
f
6
g
7
h
\.

select relname,filename,linenum,bytenum,errmsg,rawdata,rawbytes from err_copy order by linenum;
select count(*) from number; --should be 7
DROP TABLE err_copy;

-- invalid error table schema
TRUNCATE number;
create table invalid_error_table1 (a int) distributed randomly;
create table invalid_error_table3 (cmdtime timestamp with time zone, relname text, filename text, linenum integer, bytenum integer, errmsg text, rawdata text, rawbytes bytea)
		distributed by (cmdtime);
		
COPY number FROM STDIN LOG ERRORS INTO invalid_error_table1 SEGMENT REJECT LIMIT 10 ROWS; -- should fail
these are invalid line should be insert into error table.
1
\.

;

COPY number FROM STDIN LOG ERRORS INTO invalid_error_table3 SEGMENT REJECT LIMIT 10 ROWS; -- should fail
these are invalid line should be insert into error table.
1
\.

;

DROP TABLE invalid_error_table1;
DROP TABLE invalid_error_table3;

DROP TABLE number;

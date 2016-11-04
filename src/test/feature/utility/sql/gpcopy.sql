--
-- This test suite tests COPY code that is unique to greenplum db. 
--

-- ######################################################
-- different distribution columns and distribution types
-- ######################################################

CREATE TABLE copy_regression_hash1(a text, b text, c text) distributed by (b);
CREATE TABLE copy_regression_hash2(a text, b text, c text) distributed by (a,c);
CREATE TABLE copy_regression_hash3(a varchar, b int, c int, d text) distributed by (a,c,d);
CREATE TABLE copy_regression_hash4(a int[], b text) distributed by (a);
CREATE TABLE copy_regression_hash5(a text[][]) distributed by (a);
CREATE TABLE copy_regression_hash6(a int[], b text[]) distributed by (a,b);
CREATE TABLE copy_regression_hash7(a text,b text) distributed randomly;

-- single key, not first

COPY copy_regression_hash1 from stdin segment reject limit 10 rows;
a data	b data	c data
\.
COPY copy_regression_hash1(b,c,a) from stdin segment reject limit 10 rows;
b data	c data	a data
\.
SELECT * FROM copy_regression_hash1;

-- two keys

COPY copy_regression_hash2 from stdin;
a data	b data	c data
\.
COPY copy_regression_hash2(b,c,a) from stdin;
b data	c data	a data
\.
SELECT * FROM copy_regression_hash2;

-- three keys

COPY copy_regression_hash3 from stdin;
a data	2	3	d data
\.
COPY copy_regression_hash3(c,d,b,a) from stdin;
3	d data	2	a data
\.
COPY copy_regression_hash3(a,c) from stdin;
a data	3
\.
COPY copy_regression_hash3(d) from stdin;
d data
\.
SELECT * FROM copy_regression_hash3;

-- hash on int array

COPY copy_regression_hash4 from stdin;
{1,2,3,4,5}	hashed on an integer array
{10,20,30,40,50}	hashed on an integer array
\.
SELECT * FROM copy_regression_hash4 ORDER BY a;

-- hash on 2 dim varchar array

COPY copy_regression_hash5 from stdin;
{{hashing,on},{two dimentional,text array}}
\.
SELECT * FROM copy_regression_hash5 ORDER BY a;

-- hash on int array and varchar array

COPY copy_regression_hash6 from stdin;
{1,2,3,4,5}	{hashing,on,intarray,and,varchararray}
\.
SELECT * FROM copy_regression_hash6 ORDER BY a;

-- hash randomly

COPY copy_regression_hash7 from stdin;
a data	b data
a data	b data
\.
SELECT * FROM copy_regression_hash7;

DROP TABLE copy_regression_hash1;
DROP TABLE copy_regression_hash2;
DROP TABLE copy_regression_hash3;
DROP TABLE copy_regression_hash4;
DROP TABLE copy_regression_hash5;
DROP TABLE copy_regression_hash6;
DROP TABLE copy_regression_hash7;

-- ######################################################
-- default values and default functions
-- ######################################################

CREATE TABLE copy_regression_default1(a text not null default 'a default data', b text not null default 'b default data') distributed by(a);
CREATE TABLE copy_regression_default2(a text, b serial, c text) distributed by(a);
CREATE TABLE copy_regression_default3(a serial, b text, c text) distributed by(a);

-- constant defaults on key and non key attributes

COPY copy_regression_default1(a) from stdin;
a data
\.
COPY copy_regression_default1(b) from stdin;
b data
\.
SELECT * FROM copy_regression_default1;

-- non constant default on non hash key in both text and csv

COPY copy_regression_default2(a,c) from stdin;
a data	c data
a data	c data 
a data	c data
\.
COPY copy_regression_default2(a,c) from stdin csv;
a data,c data
a data,c data
a data,c data
\.
SELECT * FROM copy_regression_default2;

-- non constant default on hash key in both text and csv

COPY copy_regression_default3(b,c) from stdin;
b data	c data
b data	c data
b data	c data
\.
COPY copy_regression_default3(b,c) from stdin csv;
b data,c data
b data,c data
b data,c data
\.
SELECT * FROM copy_regression_default2;

DROP TABLE copy_regression_default1;
DROP TABLE copy_regression_default2;
DROP TABLE copy_regression_default3;

-- ######################################################
-- COPY with OIDS
-- ######################################################

CREATE TABLE copy_regression_oids(a text) WITH OIDS;

COPY copy_regression_oids from stdin with oids delimiter '|';
50000|a text data
50001|a text data
\.
COPY copy_regression_oids from stdin with oids csv;
60000,a text data
60001,a text data
\.
SELECT * FROM copy_regression_oids ORDER BY oid;

DROP TABLE copy_regression_oids;



-- ######################################################
-- COPY OUT
-- ######################################################

CREATE TABLE copy_regression_out1(a text, b varchar, c int[], d bool) distributed by(a);

-- populating table with data for copy out tests. NOTE: since we can't control the order
-- of rows COPY OUT produces from the segdb, we must have only one row for each test table.

COPY copy_regression_out1 from stdin null 'nullval';
a copyout data line 2	nullval	{2,2,2}	true
\.

-- copy out default parameters text format..
COPY copy_regression_out1 to stdout;

-- copy out '|' delimiter 'mynull' null..
COPY copy_regression_out1 to stdout with delimiter '|' null 'mynull';

-- copy out '|' delimiter "i'm null" null..
COPY copy_regression_out1 to stdout with delimiter '|' null 'i''m null';

-- copy out default parameters csv format..
COPY copy_regression_out1 to stdout with csv;

-- copy out default parameters csv format with header..
COPY copy_regression_out1 to stdout with csv header;

-- copy out partial and mixed column list (c,a,b)..
COPY copy_regression_out1(c,a,b) to stdout;

-- copy out csv default quotes and escapes and special cases..
COPY copy_regression_out1 to stdout with csv quote ''''; -- escape should be the same as quote
COPY copy_regression_out1 to stdout with csv escape '''';
COPY copy_regression_out1 to stdout with csv quote '''' escape E'\\'; -- make sure E'' is accepted

DROP TABLE copy_regression_out1;


-- ######################################################
-- Zero column table
-- ######################################################

CREATE TABLE copy_regression_nocol();

-- copy in and out of zero column table..
COPY copy_regression_nocol from stdin;
\.
COPY copy_regression_nocol from stdin;
we should get an "extra data" error here
\.
COPY copy_regression_nocol from stdin with csv;
\.
COPY copy_regression_nocol from stdin with csv; -- should fail
we should get an "extra data" error here
\.
COPY copy_regression_nocol to stdout;
COPY copy_regression_nocol to stdout with csv;

DROP TABLE copy_regression_nocol;


-- ######################################################
-- Various text format escape and default format tests
-- ######################################################

-- for text format
CREATE TABLE copy_regression_text1(a varchar ,b varchar, c text, d text, e text) distributed by(a);
CREATE TABLE copy_regression_text2(a varchar ,b varchar, c text, d text, e text) distributed by(a);
CREATE TABLE copy_regression_text3(a varchar ,b varchar, c text, d text, e text) distributed by(a);

-- column order tests

COPY copy_regression_text1 from stdin; 
a text data	b text data	c text data	d text data	e text data
\.
COPY copy_regression_text1(a,b,c,d,e) from stdin;
a text data	b text data	c text data	d text data	e text data
\.
COPY copy_regression_text1(e,d,c,b,a) from stdin;
e text data	d text data	c text data	b text data	a text data
\.
COPY copy_regression_text1(c,a,b,e,d) from stdin;
c text data	a text data	b text data	e text data	d text data
\.
COPY copy_regression_text1(a,c) from stdin;
a text data	c text data
\.
SELECT * FROM copy_regression_text1;

-- null print tests

COPY copy_regression_text2 from stdin;
a text data	\N	c text data	\N	e text data
\.
COPY copy_regression_text2 from stdin with null 'nullvalue';
a text data	nullvalue	c text data	nullvalue	nullvalue
\.
COPY copy_regression_text2 from stdin with delimiter '|' null '';
a text data||c text data||e text data
|b text data|c text data||e text data
\.
COPY copy_regression_text2 from stdin with delimiter '|' null 'i''m null';
a text data|b text data|i'm null|i'm null|e text data
\.
SELECT * FROM copy_regression_text2;

-- escape tests

COPY copy_regression_text3 from stdin with delimiter '|' escape '#';
the at sign: #100 |1|c text data|d text data|e text data 
an embedded delimiter #| character |2|c text data|d text data|e text data
a single backslash \ in col a|3|c text data|d text data|e text data
\.
COPY copy_regression_text3 from stdin with delimiter '|' escape 'off';
a single backslash \ in col a|4|c text data|d text data|e text data
c:\\file\data\neew\path|5|c text data|d text data|e text data
\.
COPY copy_regression_text3 from stdin with delimiter '|';
the at sign: \100|6|c text data|d text data|e text data
a single backslash \\ in col a|8|c text data|d text data|e text data
\.
COPY copy_regression_text3 from stdin with delimiter '|';
an embedded linefeed is not supported\
and another one that should fail\
in column a|7|c text data|d text data|e text data
\.
COPY copy_regression_text3 from stdin with delimiter '|';
an embedded linefeed sequence\nin column a|7|c text data|d text data|e text data
\.

SELECT * FROM copy_regression_text3 ORDER BY b;

DROP TABLE copy_regression_text1;
DROP TABLE copy_regression_text2;
DROP TABLE copy_regression_text3;



-- ######################################################
-- Various text format escape and default format tests
-- ######################################################

-- for csv format
CREATE TABLE copy_regression_csv1(a varchar ,b varchar, c text, d text, e text) distributed by(a);
CREATE TABLE copy_regression_csv2(a varchar ,b varchar, c text, d text, e text) distributed by(a);
CREATE TABLE copy_regression_csv3(a varchar ,b varchar, c text, d text, e text) distributed by(a);

-- column order tests

COPY copy_regression_csv1 from stdin with csv; 
a  csv data,b  csv data,c  csv data,d  csv data,e  csv data
\.
COPY copy_regression_csv1(a,b,c,d,e) from stdin with csv;
a  csv data,b  csv data,c  csv data,d  csv data,e csv  data
\.
COPY copy_regression_csv1(e,d,c,b,a) from stdin with csv;
e  csv data,d  csv data,c  csv data,b  csv data,a  csv data
\.
COPY copy_regression_csv1(c,a,b,e,d) from stdin with csv;
c  csv data,a  csv data,b  csv data,e  csv data,d  csv data
\.
COPY copy_regression_csv1(a,c) from stdin with csv;
a  csv data,c  csv data
\.
SELECT * FROM copy_regression_csv1;

-- null print tests

COPY copy_regression_csv2 from stdin with null E'\\N' csv ;
a  csv data,\N,c  csv data,\N,e  csv data
\.
COPY copy_regression_csv2 from stdin with null 'nullvalue' csv;
a  csv data,nullvalue,c  csv data,nullvalue,nullvalue
\.
COPY copy_regression_csv2 from stdin with delimiter '|' null '' csv;
a  csv data||c  csv data||e  csv data
|b  csv data|c  csv data||e  csv data
\.
SELECT * FROM copy_regression_csv2;

-- escape tests

COPY copy_regression_csv3 from stdin with csv escape E'\\';
"an embedded delimiter (comma), is printed",01,c  csv data,d  csv data,e  csv data 
"an embedded quote (doubleq)\" is printed",02,c  csv data,d  csv data,e  csv data 
"an embedded escape \\ is printed",03,c  csv data,d  csv data,e  csv data
"an embedded line feed
is printed",04,c  csv data,d  csv data,e  csv data
\.
COPY copy_regression_csv3 from stdin with delimiter E'\t' csv; --default quote and escape - "
"an embedded delimiter (tab)	 is printed"	05	c  csv data	d  csv data	e  csv data 
"an embedded quote or escape (doubleq)"" is printed"	06	c  csv data	d  csv data	e  csv data 
"an embedded line feed
is printed"	07	c  csv data	d  csv data	e  csv data
\.
COPY copy_regression_csv3 from stdin with delimiter '|' csv quote ';' escape '*';
;an embedded delimiter (pipe)| is printed;|08|c  csv data|d  csv data|e  csv data 
;an embedded quote (semicolon)*; is printed;|09|c  csv data|d  csv data|e  csv data 
;an embedded escape (asterisk)** is printed;|10|c  csv data|d  csv data|e  csv data
;an embedded line feed
is printed;|11|c  csv data|d  csv data|e  csv data
\.

-- check defaults
COPY copy_regression_csv3 from stdin with csv quote ''''; -- escape should be the same as quote
'an embedded single quote '' here',12,c csv data,d csv data,e csv data 
\.
COPY copy_regression_csv3 from stdin with csv escape '''';
"an embedded single quote '' here",13,c csv data,d csv data,e csv data 
\.
COPY copy_regression_csv3 from stdin with csv quote '''' escape E'\\'; -- make sure E'' is accepted
'an embedded backslash \\ here',14,c csv data,d csv data,e csv data 
\.
SELECT * FROM copy_regression_csv3 ORDER BY b;

DROP TABLE copy_regression_csv1;
DROP TABLE copy_regression_csv2;
DROP TABLE copy_regression_csv3;

-- ######################################################
-- FILL MISSING FIELDS
-- ######################################################

CREATE TABLE copy_regression_fill1(a int, b int, c text) distributed by(a);
CREATE TABLE copy_regression_fill2(a int, b int, c text) distributed by(c);

-- text
COPY copy_regression_fill1 from stdin with delimiter '|' fill missing fields;
1|1|one
2|2
3
\.
COPY copy_regression_fill1(c,b) from stdin with delimiter '|' fill missing fields;
one|1
two
three
\.
COPY copy_regression_fill2(a,c) from stdin with delimiter '|' fill missing fields;
1|one
2
3|three
\.
SELECT * FROM copy_regression_fill1 ORDER BY a,b,c;
SELECT * FROM copy_regression_fill2 ORDER BY a,b,c;
TRUNCATE copy_regression_fill1;
TRUNCATE copy_regression_fill2;

-- csv
COPY copy_regression_fill1 from stdin with csv delimiter '|' fill missing fields;
1|1|one
2|2
3
\.
COPY copy_regression_fill1(c,b) from stdin with csv delimiter '|' fill missing fields;
one|1
two
three
\.
COPY copy_regression_fill2(a,c) from stdin with csv delimiter '|' fill missing fields;
1|one
2
3|three
\.
SELECT * FROM copy_regression_fill1 ORDER BY a,b,c;
SELECT * FROM copy_regression_fill2 ORDER BY a,b,c;

-- empty row should fail
COPY copy_regression_fill1 from stdin with delimiter '|' fill missing fields;

\.
COPY copy_regression_fill2 from stdin with delimiter '|' fill missing fields;

\.
COPY copy_regression_fill1 from stdin with csv delimiter '|' fill missing fields;

\.
COPY copy_regression_fill2 from stdin with csv delimiter '|' fill missing fields;

\.
DROP TABLE copy_regression_fill1;
DROP TABLE copy_regression_fill2;

-- ######################################################
-- FORCE NOT NULL
-- ######################################################

CREATE TABLE copy_regression_fnn(a text, b text, c text) distributed by(a);

COPY copy_regression_fnn from stdin with csv;
one,,one
two,,
\.
SELECT * FROM copy_regression_fnn WHERE b is null order by a;
SELECT * FROM copy_regression_fnn WHERE c is null order by a;
TRUNCATE copy_regression_fnn;

COPY copy_regression_fnn from stdin with csv force not null b;
one,,one
two,,
\.
SELECT * FROM copy_regression_fnn WHERE b is null order by a;
SELECT * FROM copy_regression_fnn WHERE c is null order by a;
TRUNCATE copy_regression_fnn;

COPY copy_regression_fnn from stdin with csv force not null b,c;
one,,one
two,,
\.
SELECT * FROM copy_regression_fnn WHERE b is null order by a;
SELECT * FROM copy_regression_fnn WHERE c is null order by a;
TRUNCATE copy_regression_fnn;

-- now combine with fill missing fields
COPY copy_regression_fnn from stdin with csv fill missing fields force not null b;
one,,one
two,
\.
SELECT * FROM copy_regression_fnn WHERE b is null order by a;
SELECT * FROM copy_regression_fnn WHERE c is null order by a;

DROP TABLE copy_regression_fnn;

-- ###########################################################
-- distributed data error consolidation + original row numbers
-- ###########################################################

CREATE TABLE copy_regression_error1(a int, b int) distributed by(a);

-- parse error on QE (extra column on line 6)
-- start_ignore
COPY copy_regression_error1 from stdin;
1	1
2	2
3	3
4	4
5	5
6	6	6
7	7
\.
-- end_ignore

-- parse error on QD (missing column on line 3)
-- start_ignore
COPY copy_regression_error1 from stdin;
1	1
2	2
3
4	4
\.
-- end_ignore

-- convert error on QD (invalid type line 2)

COPY copy_regression_error1 from stdin;
1	1
two	2
3	3
\.

-- convert error on QE (invalid type line 5)
COPY copy_regression_error1 from stdin;
1	1
2	2
3	3
4	4
5	five
6	6
7	7
\.

DROP TABLE copy_regression_error1;

-- ######################################################
-- NEWLINE
-- ######################################################

CREATE TABLE copy_regression_newline(a text, b text) distributed by(a);

-- positive: text
COPY copy_regression_newline from stdin with delimiter '|' newline 'lf';
1|1
2|2
\.

-- positive: csv
COPY copy_regression_newline from stdin with delimiter '|' newline 'lf' csv;
1|1
2|2
\.

-- start_ignore
-- negative: text
COPY copy_regression_newline from stdin with delimiter '|' newline 'cr';
1|1
2|2
\.

-- negative: csv
COPY copy_regression_newline from stdin with delimiter '|' newline 'cr' csv;
1|1
2|2
\.
-- end_ignore


-- negative: invalid newline
COPY copy_regression_newline from stdin with delimiter '|' newline 'blah';
-- negative: newline not yet supported for COPY TO
COPY copy_regression_newline to stdout with delimiter '|' newline 'blah';

DROP TABLE copy_regression_newline;

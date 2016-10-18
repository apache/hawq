CREATE TABLE DATE_TBL (f1 date);
INSERT INTO DATE_TBL VALUES ('1957-04-09');

SELECT f1 FROM DATE_TBL;

SET DATESTYLE TO 'POSTGRES, MDY';
SELECT f1 FROM DATE_TBL;

SET DATESTYLE TO 'POSTGRES, DMY';
SELECT f1 FROM DATE_TBL;

--- Partially test JIRA HAWQ-1092 lc_collate and lc_ctype do not work after setting through hawq init
SELECT name, setting from pg_settings where name like 'lc%'

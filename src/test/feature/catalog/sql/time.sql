--
-- TIME
--

CREATE TABLE TIME_TBL (f1 time(2));

INSERT INTO TIME_TBL VALUES ('00:00');
INSERT INTO TIME_TBL VALUES ('01:00');
-- as of 7.4, timezone spec should be accepted and ignored
INSERT INTO TIME_TBL VALUES ('02:03 PST');
INSERT INTO TIME_TBL VALUES ('11:59 EDT');
INSERT INTO TIME_TBL VALUES ('12:00');
INSERT INTO TIME_TBL VALUES ('12:01');
INSERT INTO TIME_TBL VALUES ('23:59');
INSERT INTO TIME_TBL VALUES ('11:59:59.99 PM');

INSERT INTO TIME_TBL VALUES ('2003-03-07 15:36:39 America/New_York');
INSERT INTO TIME_TBL VALUES ('2003-07-07 15:36:39 America/New_York');
-- this should fail (the timezone offset is not known)
INSERT INTO TIME_TBL VALUES ('15:36:39 America/New_York');

SELECT f1 AS "Time" FROM TIME_TBL ORDER BY 1;

SELECT f1 AS "Three" FROM TIME_TBL WHERE f1 < '05:06:07' ORDER BY 1;

SELECT f1 AS "Five" FROM TIME_TBL WHERE f1 > '05:06:07' ORDER BY 1;

SELECT f1 AS "None" FROM TIME_TBL WHERE f1 < '00:00' ORDER BY 1;

SELECT f1 AS "Eight" FROM TIME_TBL WHERE f1 >= '00:00' ORDER BY 1;

--
-- TIME simple math
--
-- We now make a distinction between time and intervals,
-- and adding two times together makes no sense at all.
-- Leave in one query to show that it is rejected,
-- and do the rest of the testing in horology.sql
-- where we do mixed-type arithmetic. - thomas 2000-12-02

SELECT f1 + time '00:01' AS "Illegal" FROM TIME_TBL;

--
-- Support timezone_abbreviations = 'Asia'
--
-- For some reason this seems to have been missed to support
-- CST (China Standard Time). On some platforms, for example,
-- Linux, CST is used as time string suffix of GMT+8, especially
-- for users in China. In such case, the timestamp of log is
-- '14 hours' ahead of the actual China time (GMT+8).

SHOW timezone_abbreviations; -- info for debug, should always be 'Default'

SET timezone_abbreviations TO 'Asia';
SELECT abbrev, utc_offset FROM pg_timezone_abbrevs WHERE abbrev = 'CST';
SELECT '2002-09-24 10:10 CST'::TIMESTAMP WITH TIME ZONE;
SELECT '2002-09-24 10:10 Asia/Shanghai'::TIMESTAMP WITH TIME ZONE; -- won't change
SELECT '2002-09-24 10:10 GST'::TIMESTAMP WITH TIME ZONE; -- Gulf Standard Time, Asia/Dubai
SELECT '2002-09-24 10:10 IST'::TIMESTAMP WITH TIME ZONE; -- Indian Standard Time

SET timezone_abbreviations TO 'Default';
SELECT abbrev, utc_offset FROM pg_timezone_abbrevs WHERE abbrev = 'CST';
SELECT '2002-09-24 10:10 CST'::TIMESTAMP WITH TIME ZONE;
SELECT '2002-09-24 10:10 Asia/Shanghai'::TIMESTAMP WITH TIME ZONE; -- won't change
SELECT '2002-09-24 10:10 GST'::TIMESTAMP WITH TIME ZONE; -- 'GST' is invalid in Default
SELECT '2002-09-24 10:10 IST'::TIMESTAMP WITH TIME ZONE; -- Israel Standard Time

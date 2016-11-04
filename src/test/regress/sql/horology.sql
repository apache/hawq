--
-- HOROLOGY
--
-- create needed tables
CREATE TABLE ABSTIME_HOROLOGY_TBL (f1 abstime);
INSERT INTO ABSTIME_HOROLOGY_TBL (f1) VALUES ('Jan 14, 1973 03:14:21'),
(abstime 'Mon May  1 00:30:30 1995'),
(abstime 'epoch'),
(abstime 'infinity'),
(abstime '-infinity'),
(abstime 'May 10, 1947 23:59:12');

-- orca will fail for this
INSERT INTO ABSTIME_HOROLOGY_TBL (f1) VALUES('Jun 10, 1843');

CREATE TABLE INTERVAL_HOROLOGY_TBL (f1 interval);
INSERT INTO INTERVAL_HOROLOGY_TBL (f1) VALUES ('@ 1 minute'),
('@ 5 hour'),
('@ 10 day'),
('@ 34 year'),
('@ 3 months'),
('@ 14 seconds ago'),
('1 day 2 hours 3 minutes 4 seconds'),
('6 years'),
('5 months'),
('5 months 12 hours');

CREATE TABLE RELTIME_HOROLOGY_TBL (f1 reltime);
INSERT INTO RELTIME_HOROLOGY_TBL (f1) VALUES ('@ 1 minute'),
('@ 5 hour'),
('@ 10 day'),
('@ 34 year'),
('@ 3 months'),
('@ 14 seconds ago');

CREATE TABLE TIME_HOROLOGY_TBL (f1 time(2));
INSERT INTO TIME_HOROLOGY_TBL VALUES ('00:00'),
('01:00'),
('02:03 PST'),
('11:59 EDT'),
('12:00'),
('12:01'),
('23:59'),
('11:59:59.99 PM'),
('2003-03-07 15:36:39 America/New_York'),
('2003-07-07 15:36:39 America/New_York');

CREATE TABLE TIMETZ_HOROLOGY_TBL (f1 time(2) with time zone);
INSERT INTO TIMETZ_HOROLOGY_TBL VALUES ('00:01 PDT'),
('01:00 PDT'),
('02:03 PDT'),
('07:07 PST'),
('08:08 EDT'),
('11:59 PDT'),
('12:00 PDT'),
('12:01 PDT'),
('23:59 PDT'),
('11:59:59.99 PM PDT'),
('2003-03-07 15:36:39 America/New_York'),
('2003-07-07 15:36:39 America/New_York');

CREATE TABLE TIMESTAMP_HOROLOGY_TBL (d1 timestamp(2) without time zone);
INSERT INTO TIMESTAMP_HOROLOGY_TBL VALUES ('-infinity'),
('infinity'),
('epoch'),
('Mon Feb 10 17:32:01 1997 PST'),
('Mon Feb 10 17:32:01.000001 1997 PST'),
('Mon Feb 10 17:32:01.999999 1997 PST'),
('Mon Feb 10 17:32:01.4 1997 PST'),
('Mon Feb 10 17:32:01.5 1997 PST'),
('Mon Feb 10 17:32:01.6 1997 PST'),
('1997-01-02'),
('1997-01-02 03:04:05'),
('1997-02-10 17:32:01-08'),
('1997-02-10 17:32:01-0800'),
('1997-02-10 17:32:01 -08:00'),
('19970210 173201 -0800'),
('1997-06-10 17:32:01 -07:00'),
('2001-09-22T18:19:20'),
('2000-03-15 08:14:01 GMT+8'),
('2000-03-15 13:14:02 GMT-1'),
('2000-03-15 12:14:03 GMT-2'),
('2000-03-15 03:14:04 PST+8'),
('2000-03-15 02:14:05 MST+7:00'),
('Feb 10 17:32:01 1997 -0800'),
('Feb 10 17:32:01 1997'),
('Feb 10 5:32PM 1997'),
('1997/02/10 17:32:01-0800'),
('1997-02-10 17:32:01 PST'),
('Feb-10-1997 17:32:01 PST'),
('02-10-1997 17:32:01 PST'),
('19970210 173201 PST'),
('1997.041 17:32:01 UTC'),
('19970210 173201 America/New_York'),
('1997-06-10 18:32:01 PDT'),
('Feb 10 17:32:01 1997'),
('Feb 11 17:32:01 1997'),
('Feb 12 17:32:01 1997'),
('Feb 13 17:32:01 1997'),
('Feb 14 17:32:01 1997'),
('Feb 15 17:32:01 1997'),
('Feb 16 17:32:01 1997'),
('Feb 16 17:32:01 0097 BC'),
('Feb 16 17:32:01 0097'),
('Feb 16 17:32:01 0597'),
('Feb 16 17:32:01 1097'),
('Feb 16 17:32:01 1697'),
('Feb 16 17:32:01 1797'),
('Feb 16 17:32:01 1897'),
('Feb 16 17:32:01 1997'),
('Feb 16 17:32:01 2097'),
('Feb 28 17:32:01 1996'),
('Feb 29 17:32:01 1996'),
('Mar 01 17:32:01 1996'),
('Dec 30 17:32:01 1996'),
('Dec 31 17:32:01 1996'),
('Jan 01 17:32:01 1997'),
('Feb 28 17:32:01 1997'),
('Mar 01 17:32:01 1997'),
('Dec 30 17:32:01 1997'),
('Dec 31 17:32:01 1997'),
('Dec 31 17:32:01 1999'),
('Jan 01 17:32:01 2000'),
('Dec 31 17:32:01 2000'),
('Jan 01 17:32:01 2001');
set datestyle to ymd;
INSERT INTO TIMESTAMP_HOROLOGY_TBL VALUES ('97FEB10 5:32:01PM UTC'),
('97/02/10 17:32:01 UTC');
reset datestyle;

CREATE TABLE TIMESTAMPTZ_HOROLOGY_TBL (d1 timestamp(2) with time zone);
INSERT INTO TIMESTAMPTZ_HOROLOGY_TBL VALUES ('-infinity'),
('infinity'),
('epoch'),
('Mon Feb 10 17:32:01 1997 PST'),
('Mon Feb 10 17:32:01.000001 1997 PST'),
('Mon Feb 10 17:32:01.999999 1997 PST'),
('Mon Feb 10 17:32:01.4 1997 PST'),
('Mon Feb 10 17:32:01.5 1997 PST'),
('Mon Feb 10 17:32:01.6 1997 PST'),
('1997-01-02'),
('1997-01-02 03:04:05'),
('1997-02-10 17:32:01-08'),
('1997-02-10 17:32:01-0800'),
('1997-02-10 17:32:01 -08:00'),
('19970210 173201 -0800'),
('1997-06-10 17:32:01 -07:00'),
('2001-09-22T18:19:20'),
('2000-03-15 08:14:01 GMT+8'),
('2000-03-15 13:14:02 GMT-1'),
('2000-03-15 12:14:03 GMT-2'),
('2000-03-15 03:14:04 PST+8'),
('2000-03-15 02:14:05 MST+7:00'),
('Feb 10 17:32:01 1997 -0800'),
('Feb 10 17:32:01 1997'),
('Feb 10 5:32PM 1997'),
('1997/02/10 17:32:01-0800'),
('1997-02-10 17:32:01 PST'),
('Feb-10-1997 17:32:01 PST'),
('02-10-1997 17:32:01 PST'),
('19970210 173201 PST'),
('1997.041 17:32:01 UTC'),
('19970210 173201 America/New_York'),
('19970710 173201 America/New_York'),
('1997-06-10 18:32:01 PDT'),
('Feb 10 17:32:01 1997'),
('Feb 11 17:32:01 1997'),
('Feb 12 17:32:01 1997'),
('Feb 13 17:32:01 1997'),
('Feb 14 17:32:01 1997'),
('Feb 15 17:32:01 1997'),
('Feb 16 17:32:01 1997'),
('Feb 16 17:32:01 0097 BC'),
('Feb 16 17:32:01 0097'),
('Feb 16 17:32:01 0597'),
('Feb 16 17:32:01 1097'),
('Feb 16 17:32:01 1697'),
('Feb 16 17:32:01 1797'),
('Feb 16 17:32:01 1897'),
('Feb 16 17:32:01 1997'),
('Feb 16 17:32:01 2097'),
('Feb 28 17:32:01 1996'),
('Feb 29 17:32:01 1996'),
('Mar 01 17:32:01 1996'),
('Dec 30 17:32:01 1996'),
('Dec 31 17:32:01 1996'),
('Jan 01 17:32:01 1997'),
('Feb 28 17:32:01 1997'),
('Mar 01 17:32:01 1997'),
('Dec 30 17:32:01 1997'),
('Dec 31 17:32:01 1997'),
('Dec 31 17:32:01 1999'),
('Jan 01 17:32:01 2000'),
('Dec 31 17:32:01 2000'),
('Jan 01 17:32:01 2001');
set datestyle to ymd;
INSERT INTO TIMESTAMPTZ_HOROLOGY_TBL VALUES ('97FEB10 5:32:01PM UTC'),
('97/02/10 17:32:01 UTC');
reset datestyle;

--
--
--
SET DateStyle = 'Postgres, MDY';

--
-- Test various input formats
--
SELECT timestamp with time zone '20011227 040506+08';
SELECT timestamp with time zone '20011227 040506-08';
SELECT timestamp with time zone '20011227 040506.789+08';
SELECT timestamp with time zone '20011227 040506.789-08';
SELECT timestamp with time zone '20011227T040506+08';
SELECT timestamp with time zone '20011227T040506-08';
SELECT timestamp with time zone '20011227T040506.789+08';
SELECT timestamp with time zone '20011227T040506.789-08';
SELECT timestamp with time zone '2001-12-27 04:05:06.789-08';
SELECT timestamp with time zone '2001.12.27 04:05:06.789-08';
SELECT timestamp with time zone '2001/12/27 04:05:06.789-08';
SELECT timestamp with time zone '12/27/2001 04:05:06.789-08';
-- should fail in mdy mode:
SELECT timestamp with time zone '27/12/2001 04:05:06.789-08';
set datestyle to dmy;
SELECT timestamp with time zone '27/12/2001 04:05:06.789-08';
reset datestyle;
SELECT timestamp with time zone 'Y2001M12D27H04M05S06.789+08';
SELECT timestamp with time zone 'Y2001M12D27H04M05S06.789-08';
SELECT timestamp with time zone 'Y2001M12D27H04MM05S06.789+08';
SELECT timestamp with time zone 'Y2001M12D27H04MM05S06.789-08';
SELECT timestamp with time zone 'J2452271+08';
SELECT timestamp with time zone 'J2452271-08';
SELECT timestamp with time zone 'J2452271.5+08';
SELECT timestamp with time zone 'J2452271.5-08';
SELECT timestamp with time zone 'J2452271 04:05:06+08';
SELECT timestamp with time zone 'J2452271 04:05:06-08';
SELECT timestamp with time zone 'J2452271T040506+08';
SELECT timestamp with time zone 'J2452271T040506-08';
SELECT timestamp with time zone 'J2452271T040506.789+08';
SELECT timestamp with time zone 'J2452271T040506.789-08';
-- German/European-style dates with periods as delimiters
SELECT timestamp with time zone '12.27.2001 04:05:06.789+08';
SELECT timestamp with time zone '12.27.2001 04:05:06.789-08';
SET DateStyle = 'German';
SELECT timestamp with time zone '27.12.2001 04:05:06.789+08';
SELECT timestamp with time zone '27.12.2001 04:05:06.789-08';
SET DateStyle = 'ISO';
-- As of 7.4, allow time without time zone having a time zone specified
SELECT time without time zone '040506.789+08';
SELECT time without time zone '040506.789-08';
SELECT time without time zone 'T040506.789+08';
SELECT time without time zone 'T040506.789-08';
SELECT time with time zone '040506.789+08';
SELECT time with time zone '040506.789-08';
SELECT time with time zone 'T040506.789+08';
SELECT time with time zone 'T040506.789-08';
SELECT time with time zone 'T040506.789 +08';
SELECT time with time zone 'T040506.789 -08';
SET DateStyle = 'Postgres, MDY';

--
-- date, time arithmetic
--

SELECT date '1981-02-03' + time '04:05:06' AS "Date + Time";
SELECT date '1991-02-03' + time with time zone '04:05:06 PST' AS "Date + Time PST";
SELECT date '2001-02-03' + time with time zone '04:05:06 UTC' AS "Date + Time UTC";
SELECT date '1991-02-03' + interval '2 years' AS "Add Two Years";
SELECT date '2001-12-13' - interval '2 years' AS "Subtract Two Years";
-- subtract time from date should not make sense; use interval instead
SELECT date '1991-02-03' - time '04:05:06' AS "Subtract Time";
SELECT date '1991-02-03' - time with time zone '04:05:06 UTC' AS "Subtract Time UTC";

--
-- timestamp, interval arithmetic
--

SELECT timestamp without time zone '1996-03-01' - interval '1 second' AS "Feb 29";
SELECT timestamp without time zone '1999-03-01' - interval '1 second' AS "Feb 28";
SELECT timestamp without time zone '2000-03-01' - interval '1 second' AS "Feb 29";
SELECT timestamp without time zone '1999-12-01' + interval '1 month - 1 second' AS "Dec 31";
SELECT timestamp without time zone 'Jan 1, 4713 BC' + interval '106000000 days' AS "Feb 23, 285506";
SELECT timestamp without time zone 'Jan 1, 4713 BC' + interval '107000000 days' AS "Jan 20, 288244";
SELECT timestamp without time zone 'Jan 1, 4713 BC' + interval '109203489 days' AS "Dec 31, 294276";
SELECT timestamp without time zone '12/31/294276' - timestamp without time zone '12/23/1999' AS "106751991 Days";

-- Shorthand values
-- Not directly usable for regression testing since these are not constants.
-- So, just try to test parser and hope for the best - thomas 97/04/26
SELECT (timestamp without time zone 'today' = (timestamp without time zone 'yesterday' + interval '1 day')) as "True";
SELECT (timestamp without time zone 'today' = (timestamp without time zone 'tomorrow' - interval '1 day')) as "True";
SELECT (timestamp without time zone 'tomorrow' = (timestamp without time zone 'yesterday' + interval '2 days')) as "True";
SELECT (timestamp without time zone 'tomorrow' > 'now') as "True";

-- Convert from date and time to timestamp
-- This test used to be timestamp(date,time) but no longer allowed by grammar
-- to enable support for SQL99 timestamp type syntax.
SELECT date '1994-01-01' + time '11:00' AS "Jan_01_1994_11am";
SELECT date '1994-01-01' + time '10:00' AS "Jan_01_1994_10am";
SELECT date '1994-01-01' + timetz '11:00-5' AS "Jan_01_1994_8am";
SELECT timestamptz(date '1994-01-01', time with time zone '11:00-5') AS "Jan_01_1994_8am";

SELECT '' AS "64", d1 + interval '1 year' AS one_year FROM TIMESTAMP_HOROLOGY_TBL ORDER BY 2;
SELECT '' AS "64", d1 - interval '1 year' AS one_year FROM TIMESTAMP_HOROLOGY_TBL ORDER BY 2;

SELECT timestamp with time zone '1996-03-01' - interval '1 second' AS "Feb 29";
SELECT timestamp with time zone '1999-03-01' - interval '1 second' AS "Feb 28";
SELECT timestamp with time zone '2000-03-01' - interval '1 second' AS "Feb 29";
SELECT timestamp with time zone '1999-12-01' + interval '1 month - 1 second' AS "Dec 31";

SELECT (timestamp with time zone 'today' = (timestamp with time zone 'yesterday' + interval '1 day')) as "True";
SELECT (timestamp with time zone 'today' = (timestamp with time zone 'tomorrow' - interval '1 day')) as "True";
SELECT (timestamp with time zone 'tomorrow' = (timestamp with time zone 'yesterday' + interval '2 days')) as "True";
SELECT (timestamp with time zone 'tomorrow' > 'now') as "True";


-- timestamp with time zone, interval arithmetic around DST change
SET TIME ZONE 'CST7CDT';
SELECT timestamp with time zone '2005-04-02 12:00-07' + interval '1 day' as "Apr 3, 12:00";
SELECT timestamp with time zone '2005-04-02 12:00-07' + interval '24 hours' as "Apr 3, 13:00";
SELECT timestamp with time zone '2005-04-03 12:00-06' - interval '1 day' as "Apr 2, 12:00";
SELECT timestamp with time zone '2005-04-03 12:00-06' - interval '24 hours' as "Apr 2, 11:00";
RESET TIME ZONE;
SET TIME ZONE 'PST8PDT';

SELECT timestamptz(date '1994-01-01', time '11:00') AS "Jan_01_1994_10am";
SELECT timestamptz(date '1994-01-01', time '10:00') AS "Jan_01_1994_9am";
SELECT timestamptz(date '1994-01-01', time with time zone '11:00-8') AS "Jan_01_1994_11am";
SELECT timestamptz(date '1994-01-01', time with time zone '10:00-8') AS "Jan_01_1994_10am";
SELECT timestamptz(date '1994-01-01', time with time zone '11:00-5') AS "Jan_01_1994_8am";

SELECT '' AS "64", d1 + interval '1 year' AS one_year FROM TIMESTAMPTZ_HOROLOGY_TBL ORDER BY 2;
SELECT '' AS "64", d1 - interval '1 year' AS one_year FROM TIMESTAMPTZ_HOROLOGY_TBL ORDER BY 2;

--
-- time, interval arithmetic
--

SELECT CAST(time '01:02' AS interval) AS "+01:02";
SELECT CAST(interval '02:03' AS time) AS "02:03:00";
SELECT time '01:30' + interval '02:01' AS "03:31:00";
SELECT time '01:30' - interval '02:01' AS "23:29:00";
SELECT time '02:30' + interval '36:01' AS "14:31:00";
SELECT time '03:30' + interval '1 month 04:01' AS "07:31:00";
SELECT CAST(time with time zone '01:02-08' AS interval) AS "+00:01";
SELECT CAST(interval '02:03' AS time with time zone) AS "02:03:00-08";
SELECT time with time zone '01:30-08' - interval '02:01' AS "23:29:00-08";
SELECT time with time zone '02:30-08' + interval '36:01' AS "14:31:00-08";

-- These two tests cannot be used because they default to current timezone,
-- which may be either -08 or -07 depending on the time of year.
-- SELECT time with time zone '01:30' + interval '02:01' AS "03:31:00-08";
-- SELECT time with time zone '03:30' + interval '1 month 04:01' AS "07:31:00-08";
-- Try the following two tests instead, as a poor substitute

SELECT CAST(CAST(date 'today' + time with time zone '05:30'
            + interval '02:01' AS time with time zone) AS time) AS "07:31:00";

SELECT CAST(cast(date 'today' + time with time zone '03:30'
  + interval '1 month 04:01' as timestamp without time zone) AS time) AS "07:31:00";

SELECT t.d1 + i.f1 AS "102" FROM TIMESTAMP_HOROLOGY_TBL t, INTERVAL_HOROLOGY_TBL i
  WHERE t.d1 BETWEEN '1990-01-01' AND '2001-01-01'
    AND i.f1 BETWEEN '00:00' AND '23:00' ORDER BY 1;

SELECT t.d1 - i.f1 AS "102" FROM TIMESTAMP_HOROLOGY_TBL t, INTERVAL_HOROLOGY_TBL i
  WHERE t.d1 BETWEEN '1990-01-01' AND '2001-01-01'
    AND i.f1 BETWEEN '00:00' AND '23:00' ORDER BY 1;

SELECT t.f1 + i.f1 AS "80" FROM TIME_HOROLOGY_TBL t, INTERVAL_HOROLOGY_TBL i ORDER BY 1;
SELECT t.f1 - i.f1 AS "80" FROM TIME_HOROLOGY_TBL t, INTERVAL_HOROLOGY_TBL i ORDER BY 1;

SELECT t.f1 + i.f1 AS "100" FROM TIMETZ_HOROLOGY_TBL t, INTERVAL_HOROLOGY_TBL i ORDER BY 1;
SELECT t.f1 - i.f1 AS "100" FROM TIMETZ_HOROLOGY_TBL t, INTERVAL_HOROLOGY_TBL i ORDER BY 1;

-- SQL9x OVERLAPS operator
-- test with time zone
SELECT (timestamp with time zone '2000-11-27', timestamp with time zone '2000-11-28')
  OVERLAPS (timestamp with time zone '2000-11-27 12:00', timestamp with time zone '2000-11-30') AS "True";

SELECT (timestamp with time zone '2000-11-26', timestamp with time zone '2000-11-27')
  OVERLAPS (timestamp with time zone '2000-11-27 12:00', timestamp with time zone '2000-11-30') AS "False";

SELECT (timestamp with time zone '2000-11-27', timestamp with time zone '2000-11-28')
  OVERLAPS (timestamp with time zone '2000-11-27 12:00', interval '1 day') AS "True";

SELECT (timestamp with time zone '2000-11-27', interval '12 hours')
  OVERLAPS (timestamp with time zone '2000-11-27 12:00', timestamp with time zone '2000-11-30') AS "False";

SELECT (timestamp with time zone '2000-11-27', interval '12 hours')
  OVERLAPS (timestamp with time zone '2000-11-27', interval '12 hours') AS "True";

SELECT (timestamp with time zone '2000-11-27', interval '12 hours')
  OVERLAPS (timestamp with time zone '2000-11-27 12:00', interval '12 hours') AS "False";

-- test without time zone
SELECT (timestamp without time zone '2000-11-27', timestamp without time zone '2000-11-28')
  OVERLAPS (timestamp without time zone '2000-11-27 12:00', timestamp without time zone '2000-11-30') AS "True";

SELECT (timestamp without time zone '2000-11-26', timestamp without time zone '2000-11-27')
  OVERLAPS (timestamp without time zone '2000-11-27 12:00', timestamp without time zone '2000-11-30') AS "False";

SELECT (timestamp without time zone '2000-11-27', timestamp without time zone '2000-11-28')
  OVERLAPS (timestamp without time zone '2000-11-27 12:00', interval '1 day') AS "True";

SELECT (timestamp without time zone '2000-11-27', interval '12 hours')
  OVERLAPS (timestamp without time zone '2000-11-27 12:00', timestamp without time zone '2000-11-30') AS "False";

SELECT (timestamp without time zone '2000-11-27', interval '12 hours')
  OVERLAPS (timestamp without time zone '2000-11-27', interval '12 hours') AS "True";

SELECT (timestamp without time zone '2000-11-27', interval '12 hours')
  OVERLAPS (timestamp without time zone '2000-11-27 12:00', interval '12 hours') AS "False";

-- test time and interval
SELECT (time '00:00', time '01:00')
  OVERLAPS (time '00:30', time '01:30') AS "True";

SELECT (time '00:00', interval '1 hour')
  OVERLAPS (time '00:30', interval '1 hour') AS "True";

SELECT (time '00:00', interval '1 hour')
  OVERLAPS (time '01:30', interval '1 hour') AS "False";

-- SQL99 seems to want this to be false (and we conform to the spec).
-- istm that this *should* return true, on the theory that time
-- intervals can wrap around the day boundary - thomas 2001-09-25
SELECT (time '00:00', interval '1 hour')
  OVERLAPS (time '01:30', interval '1 day') AS "False";

CREATE TABLE TEMP_TIMESTAMP (f1 timestamp with time zone);

-- get some candidate input values

INSERT INTO TEMP_TIMESTAMP (f1)
  SELECT d1 FROM TIMESTAMP_HOROLOGY_TBL
  WHERE d1 BETWEEN '13-jun-1957' AND '1-jan-1997'
   OR d1 BETWEEN '1-jan-1999' AND '1-jan-2010';

SELECT '' AS "16", f1 AS "timestamp"
  FROM TEMP_TIMESTAMP
  ORDER BY "timestamp";

SELECT '' AS "160", d.f1 AS "timestamp", t.f1 AS "interval", d.f1 + t.f1 AS plus
  FROM TEMP_TIMESTAMP d, INTERVAL_HOROLOGY_TBL t
  ORDER BY plus, "timestamp", "interval";

SELECT '' AS "160", d.f1 AS "timestamp", t.f1 AS "interval", d.f1 - t.f1 AS minus
  FROM TEMP_TIMESTAMP d, INTERVAL_HOROLOGY_TBL t
  WHERE isfinite(d.f1)
  ORDER BY minus, "timestamp", "interval";

SELECT '' AS "16", d.f1 AS "timestamp",
   timestamp with time zone '1980-01-06 00:00 GMT' AS gpstime_zero,
   d.f1 - timestamp with time zone '1980-01-06 00:00 GMT' AS difference
  FROM TEMP_TIMESTAMP d
  ORDER BY difference;

SELECT '' AS "226", d1.f1 AS timestamp1, d2.f1 AS timestamp2, d1.f1 - d2.f1 AS difference
  FROM TEMP_TIMESTAMP d1, TEMP_TIMESTAMP d2
  ORDER BY timestamp1, timestamp2, difference;

--
-- abstime, reltime arithmetic
--

SELECT '' AS ten, ABSTIME_HOROLOGY_TBL.f1 AS abstime, RELTIME_HOROLOGY_TBL.f1 AS reltime
    FROM ABSTIME_HOROLOGY_TBL, RELTIME_HOROLOGY_TBL
   WHERE (ABSTIME_HOROLOGY_TBL.f1 + RELTIME_HOROLOGY_TBL.f1) < abstime 'Jan 14 14:00:00 1971'
   ORDER BY abstime, reltime;

-- these four queries should return the same answer
-- the "infinity" and "-infinity" tuples in ABSTIME_HOROLOGY_TBL cannot be added and
-- therefore, should not show up in the results.

SELECT '' AS three, * FROM ABSTIME_HOROLOGY_TBL
  WHERE  (ABSTIME_HOROLOGY_TBL.f1 + reltime '@ 3 year')         -- +3 years
    < abstime 'Jan 14 14:00:00 1977' ORDER BY 2;

SELECT '' AS three, * FROM ABSTIME_HOROLOGY_TBL
   WHERE  (ABSTIME_HOROLOGY_TBL.f1 + reltime '@ 3 year ago')    -- -3 years
     < abstime 'Jan 14 14:00:00 1971' ORDER BY 2;

SELECT '' AS three, * FROM ABSTIME_HOROLOGY_TBL
   WHERE  (ABSTIME_HOROLOGY_TBL.f1 - reltime '@ 3 year')        -- -(+3) years
    < abstime 'Jan 14 14:00:00 1971' ORDER BY 2;

SELECT '' AS three, * FROM ABSTIME_HOROLOGY_TBL
   WHERE  (ABSTIME_HOROLOGY_TBL.f1 - reltime '@ 3 year ago')    -- -(-3) years
     < abstime 'Jan 14 14:00:00 1977' ORDER BY 2;

--
-- Conversions
--

SELECT '' AS "16", f1 AS "timestamp", date(f1) AS date
  FROM TEMP_TIMESTAMP
  WHERE f1 <> timestamp 'now'
  ORDER BY date, "timestamp";

SELECT '' AS "16", f1 AS "timestamp", abstime(f1) AS abstime
  FROM TEMP_TIMESTAMP
  ORDER BY abstime;

SELECT '' AS four, f1 AS abstime, date(f1) AS date
  FROM ABSTIME_HOROLOGY_TBL
  WHERE isfinite(f1) AND f1 <> abstime 'now'
  ORDER BY date, abstime;

SELECT '' AS two, d1 AS "timestamp", abstime(d1) AS abstime
  FROM TIMESTAMP_HOROLOGY_TBL WHERE NOT isfinite(d1) ORDER BY 2;

SELECT '' AS three, f1 as abstime, cast(f1 as timestamp) AS "timestamp"
  FROM ABSTIME_HOROLOGY_TBL WHERE NOT isfinite(f1) ORDER BY 2;

SELECT '' AS ten, f1 AS interval, reltime(f1) AS reltime
  FROM INTERVAL_HOROLOGY_TBL ORDER BY 2;

SELECT '' AS six, f1 as reltime, CAST(f1 AS interval) AS interval
  FROM RELTIME_HOROLOGY_TBL ORDER BY 2;

DROP TABLE TEMP_TIMESTAMP;

--
-- Formats
--

SET DateStyle TO 'US,Postgres';

SHOW DateStyle;

SELECT '' AS "64", d1 AS us_postgres FROM TIMESTAMP_HOROLOGY_TBL ORDER BY 2;

SELECT '' AS seven, f1 AS us_postgres FROM ABSTIME_HOROLOGY_TBL ORDER BY 2;

SET DateStyle TO 'US,ISO';

SELECT '' AS "64", d1 AS us_iso FROM TIMESTAMP_HOROLOGY_TBL ORDER BY 2;

SELECT '' AS seven, f1 AS us_iso FROM ABSTIME_HOROLOGY_TBL ORDER BY 2;

SET DateStyle TO 'US,SQL';

SHOW DateStyle;

SELECT '' AS "64", d1 AS us_sql FROM TIMESTAMP_HOROLOGY_TBL ORDER BY 2;

SELECT '' AS seven, f1 AS us_sql FROM ABSTIME_HOROLOGY_TBL ORDER BY 2;

SET DateStyle TO 'European,Postgres';

SHOW DateStyle;

INSERT INTO TIMESTAMP_HOROLOGY_TBL VALUES('13/06/1957');

SELECT count(*) as one FROM TIMESTAMP_HOROLOGY_TBL WHERE d1 = 'Jun 13 1957';

SELECT '' AS "65", d1 AS european_postgres FROM TIMESTAMP_HOROLOGY_TBL ORDER BY 2;

SELECT '' AS seven, f1 AS european_postgres FROM ABSTIME_HOROLOGY_TBL ORDER BY 2;

SET DateStyle TO 'European,ISO';

SHOW DateStyle;

SELECT '' AS "65", d1 AS european_iso FROM TIMESTAMP_HOROLOGY_TBL ORDER BY 2;

SELECT '' AS seven, f1 AS european_iso FROM ABSTIME_HOROLOGY_TBL ORDER BY 2;

SET DateStyle TO 'European,SQL';

SHOW DateStyle;

SELECT '' AS "65", d1 AS european_sql FROM TIMESTAMP_HOROLOGY_TBL ORDER BY 2;

SELECT '' AS seven, f1 AS european_sql FROM ABSTIME_HOROLOGY_TBL ORDER BY 2;

RESET DateStyle;

--
-- to_timestamp()
--

SELECT to_timestamp('0097/Feb/16 --> 08:14:30', 'YYYY/Mon/DD --> HH:MI:SS');

SELECT to_timestamp('97/2/16 8:14:30', 'FMYYYY/FMMM/FMDD FMHH:FMMI:FMSS');

SELECT to_timestamp('1985 January 12', 'YYYY FMMonth DD');

SELECT to_timestamp('My birthday-> Year: 1976, Month: May, Day: 16',
                    '"My birthday-> Year" YYYY, "Month:" FMMonth, "Day:" DD');

SELECT to_timestamp('1,582nd VIII 21', 'Y,YYYth FMRM DD');

SELECT to_timestamp('15 "text between quote marks" 98 54 45',
                    E'HH24 "\\text between quote marks\\"" YY MI SS');

SELECT to_timestamp('05121445482000', 'MMDDHH24MISSYYYY');

SELECT to_timestamp('2000January09Sunday', 'YYYYFMMonthDDFMDay');

SELECT to_timestamp('97/Feb/16', 'YYMonDD');

SELECT to_timestamp('19971116', 'YYYYMMDD');

SELECT to_timestamp('20000-1116', 'YYYY-MMDD');

SELECT to_timestamp('9-1116', 'Y-MMDD');

SELECT to_timestamp('95-1116', 'YY-MMDD');

SELECT to_timestamp('995-1116', 'YYY-MMDD');

SELECT to_timestamp('2005426', 'YYYYWWD');

SELECT to_timestamp('2005300', 'YYYYDDD');

SELECT to_timestamp('2005527', 'IYYYIWID');

SELECT to_timestamp('005527', 'IYYIWID');

SELECT to_timestamp('05527', 'IYIWID');

SELECT to_timestamp('5527', 'IIWID');

SELECT to_timestamp('2005364', 'IYYYIDDD');

SELECT to_timestamp('20050302', 'YYYYMMDD');

SELECT to_timestamp('2005 03 02', 'YYYYMMDD');

SELECT to_timestamp(' 2005 03 02', 'YYYYMMDD');

SELECT to_timestamp('  20050302', 'YYYYMMDD');

--
-- Check errors for some incorrect usages of to_timestamp()
--

-- Mixture of date conventions (ISO week and Gregorian):
SELECT to_timestamp('2005527', 'YYYYIWID');

-- Insufficient characters in the source string:
SELECT to_timestamp('19971', 'YYYYMMDD');

-- Insufficient digit characters for a single node:
SELECT to_timestamp('19971)24', 'YYYYMMDD');

-- Value clobbering:
SELECT to_timestamp('1997-11-Jan-16', 'YYYY-MM-Mon-DD');

-- Non-numeric input:
SELECT to_timestamp('199711xy', 'YYYYMMDD');

-- Input that doesn't fit in an int:
SELECT to_timestamp('10000000000', 'FMYYYY');

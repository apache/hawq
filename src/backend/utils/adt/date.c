/*-------------------------------------------------------------------------
 *
 * date.c
 *	  implements DATE and TIME data types specified in SQL-92 standard
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/utils/adt/date.c,v 1.125.2.1 2007/06/02 16:41:15 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>
#include <limits.h>
#include <float.h>
#include <time.h>

#include "access/hash.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/nabstime.h"

/*
 * gcc's -ffast-math switch breaks routines that expect exact results from
 * expressions like timeval / SECS_PER_HOUR, where timeval is double.
 */
#ifdef __FAST_MATH__
#error -ffast-math is known to break this code
#endif


static int	time2tm(TimeADT time, struct pg_tm * tm, fsec_t *fsec);
static int	timetz2tm(TimeTzADT *time, struct pg_tm * tm, fsec_t *fsec, int *tzp);
static int	tm2time(struct pg_tm * tm, fsec_t fsec, TimeADT *result);
static int	tm2timetz(struct pg_tm * tm, fsec_t fsec, int tz, TimeTzADT *result);
static void AdjustTimeForTypmod(TimeADT *time, int32 typmod);

/*****************************************************************************
 *	 Date ADT
 *****************************************************************************/


/* date_in()
 * Given date text string, convert to internal date format.
 */
Datum
date_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	DateADT		date;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;
	int			tzp;
	int			dtype = 0;
	int			nf;
	int			dterr;
	char	   *field[MAXDATEFIELDS];
	int			ftype[MAXDATEFIELDS];
	char		workbuf[MAXDATELEN + 1];

	dterr = ParseDateTime(str, workbuf, sizeof(workbuf),
						  field, ftype, MAXDATEFIELDS, &nf);
	if (dterr == 0)
		dterr = DecodeDateTime(field, ftype, nf, &dtype, tm, &fsec, &tzp);
	if (dterr != 0)
		DateTimeParseError(dterr, str, "date");

	switch (dtype)
	{
		case DTK_DATE:
			break;

		case DTK_CURRENT:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			  errmsg("date/time value \"current\" is no longer supported")));

			GetCurrentDateTime(tm);
			break;

		case DTK_EPOCH:
			GetEpochTime(tm);
			break;

		default:
			DateTimeParseError(DTERR_BAD_FORMAT, str, "date");
			break;
	}

	if (!IS_VALID_JULIAN(tm->tm_year, tm->tm_mon, tm->tm_mday))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("date out of range: \"%s\"", str)));

	date = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - POSTGRES_EPOCH_JDATE;

	PG_RETURN_DATEADT(date);
}

/* date_out()
 * Given internal format date, convert to text string.
 */
Datum
date_out(PG_FUNCTION_ARGS)
{
	DateADT		date = PG_GETARG_DATEADT(0);
	char	   *result;
	struct pg_tm tt,
			   *tm = &tt;
	char		buf[MAXDATELEN + 1];

	j2date(date + POSTGRES_EPOCH_JDATE,
		   &(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));

	EncodeDateOnly(tm, DateStyle, buf);

	result = pstrdup(buf);
	PG_RETURN_CSTRING(result);
}

/*
 *		date_recv			- converts external binary format to date
 */
Datum
date_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

	PG_RETURN_DATEADT((DateADT) pq_getmsgint(buf, sizeof(DateADT)));
}

/*
 *		date_send			- converts date to binary format
 */
Datum
date_send(PG_FUNCTION_ARGS)
{
	DateADT		date = PG_GETARG_DATEADT(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint(&buf, date, sizeof(date));
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


/*
 * Comparison functions for dates
 */

Datum
date_eq(PG_FUNCTION_ARGS)
{
	DateADT		dateVal1 = PG_GETARG_DATEADT(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);

	PG_RETURN_BOOL(dateVal1 == dateVal2);
}

Datum
date_ne(PG_FUNCTION_ARGS)
{
	DateADT		dateVal1 = PG_GETARG_DATEADT(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);

	PG_RETURN_BOOL(dateVal1 != dateVal2);
}

Datum
date_lt(PG_FUNCTION_ARGS)
{
	DateADT		dateVal1 = PG_GETARG_DATEADT(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);

	PG_RETURN_BOOL(dateVal1 < dateVal2);
}

Datum
date_le(PG_FUNCTION_ARGS)
{
	DateADT		dateVal1 = PG_GETARG_DATEADT(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);

	PG_RETURN_BOOL(dateVal1 <= dateVal2);
}

Datum
date_gt(PG_FUNCTION_ARGS)
{
	DateADT		dateVal1 = PG_GETARG_DATEADT(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);

	PG_RETURN_BOOL(dateVal1 > dateVal2);
}

Datum
date_ge(PG_FUNCTION_ARGS)
{
	DateADT		dateVal1 = PG_GETARG_DATEADT(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);

	PG_RETURN_BOOL(dateVal1 >= dateVal2);
}

Datum
date_cmp(PG_FUNCTION_ARGS)
{
	DateADT		dateVal1 = PG_GETARG_DATEADT(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);

	if (dateVal1 < dateVal2)
		PG_RETURN_INT32(-1);
	else if (dateVal1 > dateVal2)
		PG_RETURN_INT32(1);
	PG_RETURN_INT32(0);
}

Datum
date_larger(PG_FUNCTION_ARGS)
{
	DateADT		dateVal1 = PG_GETARG_DATEADT(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);

	PG_RETURN_DATEADT((dateVal1 > dateVal2) ? dateVal1 : dateVal2);
}

Datum
date_smaller(PG_FUNCTION_ARGS)
{
	DateADT		dateVal1 = PG_GETARG_DATEADT(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);

	PG_RETURN_DATEADT((dateVal1 < dateVal2) ? dateVal1 : dateVal2);
}

/* Compute difference between two dates in days.
 */
Datum
date_mi(PG_FUNCTION_ARGS)
{
	DateADT		dateVal1 = PG_GETARG_DATEADT(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);

	PG_RETURN_INT32((int32) (dateVal1 - dateVal2));
}

/* Add a number of days to a date, giving a new date.
 * Must handle both positive and negative numbers of days.
 */
Datum
date_pli(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	int32		days = PG_GETARG_INT32(1);

	PG_RETURN_DATEADT(dateVal + days);
}

/* Subtract a number of days from a date, giving a new date.
 */
Datum
date_mii(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	int32		days = PG_GETARG_INT32(1);

	PG_RETURN_DATEADT(dateVal - days);
}

/*
 * Internal routines for promoting date to timestamp and timestamp with
 * time zone
 */

static Timestamp
date2timestamp(DateADT dateVal)
{
	Timestamp result;

#ifdef HAVE_INT64_TIMESTAMP
	/* date is days since 2000, timestamp is microseconds since same... */
	result = dateVal * USECS_PER_DAY;
		/* Date's range is wider than timestamp's, so check for overflow */
	if (result / USECS_PER_DAY != dateVal)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("date out of range for timestamp")));
#else
	/* date is days since 2000, timestamp is seconds since same... */
	result = dateVal * (double) SECS_PER_DAY;
#endif

	return result;
}

static TimestampTz
date2timestamptz(DateADT dateVal)
{
	TimestampTz result;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;

	j2date(dateVal + POSTGRES_EPOCH_JDATE,
		   &(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));

	tm->tm_hour = 0;
	tm->tm_min = 0;
	tm->tm_sec = 0;
		tz = DetermineTimeZoneOffset(tm, session_timezone);

#ifdef HAVE_INT64_TIMESTAMP
	result = dateVal * USECS_PER_DAY + tz * USECS_PER_SEC;
		/* Date's range is wider than timestamp's, so check for overflow */
	if ((result - tz * USECS_PER_SEC) / USECS_PER_DAY != dateVal)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("date out of range for timestamp")));
#else
	result = dateVal * (double) SECS_PER_DAY + tz;
#endif

	return result;
}


/*
 * Crosstype comparison functions for dates
 */

Datum
date_eq_timestamp(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	Timestamp	dt1;

	dt1 = date2timestamp(dateVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) == 0);
}

Datum
date_ne_timestamp(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	Timestamp	dt1;

	dt1 = date2timestamp(dateVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) != 0);
}

Datum
date_lt_timestamp(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	Timestamp	dt1;

	dt1 = date2timestamp(dateVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) < 0);
}

Datum
date_gt_timestamp(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	Timestamp	dt1;

	dt1 = date2timestamp(dateVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) > 0);
}

Datum
date_le_timestamp(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	Timestamp	dt1;

	dt1 = date2timestamp(dateVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) <= 0);
}

Datum
date_ge_timestamp(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	Timestamp	dt1;

	dt1 = date2timestamp(dateVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) >= 0);
}

Datum
date_cmp_timestamp(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	Timestamp	dt1;

	dt1 = date2timestamp(dateVal);

	PG_RETURN_INT32(timestamp_cmp_internal(dt1, dt2));
}

Datum
date_eq_timestamptz(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz dt1;

	dt1 = date2timestamptz(dateVal);

	PG_RETURN_BOOL(timestamptz_cmp_internal(dt1, dt2) == 0);
}

Datum
date_ne_timestamptz(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz dt1;

	dt1 = date2timestamptz(dateVal);

	PG_RETURN_BOOL(timestamptz_cmp_internal(dt1, dt2) != 0);
}

Datum
date_lt_timestamptz(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz dt1;

	dt1 = date2timestamptz(dateVal);

	PG_RETURN_BOOL(timestamptz_cmp_internal(dt1, dt2) < 0);
}

Datum
date_gt_timestamptz(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz dt1;

	dt1 = date2timestamptz(dateVal);

	PG_RETURN_BOOL(timestamptz_cmp_internal(dt1, dt2) > 0);
}

Datum
date_le_timestamptz(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz dt1;

	dt1 = date2timestamptz(dateVal);

	PG_RETURN_BOOL(timestamptz_cmp_internal(dt1, dt2) <= 0);
}

Datum
date_ge_timestamptz(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz dt1;

	dt1 = date2timestamptz(dateVal);

	PG_RETURN_BOOL(timestamptz_cmp_internal(dt1, dt2) >= 0);
}

Datum
date_cmp_timestamptz(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz dt1;

	dt1 = date2timestamptz(dateVal);

	PG_RETURN_INT32(timestamptz_cmp_internal(dt1, dt2));
}

Datum
timestamp_eq_date(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	DateADT		dateVal = PG_GETARG_DATEADT(1);
	Timestamp	dt2;

	dt2 = date2timestamp(dateVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) == 0);
}

Datum
timestamp_ne_date(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	DateADT		dateVal = PG_GETARG_DATEADT(1);
	Timestamp	dt2;

	dt2 = date2timestamp(dateVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) != 0);
}

Datum
timestamp_lt_date(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	DateADT		dateVal = PG_GETARG_DATEADT(1);
	Timestamp	dt2;

	dt2 = date2timestamp(dateVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) < 0);
}

Datum
timestamp_gt_date(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	DateADT		dateVal = PG_GETARG_DATEADT(1);
	Timestamp	dt2;

	dt2 = date2timestamp(dateVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) > 0);
}

Datum
timestamp_le_date(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	DateADT		dateVal = PG_GETARG_DATEADT(1);
	Timestamp	dt2;

	dt2 = date2timestamp(dateVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) <= 0);
}

Datum
timestamp_ge_date(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	DateADT		dateVal = PG_GETARG_DATEADT(1);
	Timestamp	dt2;

	dt2 = date2timestamp(dateVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) >= 0);
}

Datum
timestamp_cmp_date(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	DateADT		dateVal = PG_GETARG_DATEADT(1);
	Timestamp	dt2;

	dt2 = date2timestamp(dateVal);

	PG_RETURN_INT32(timestamp_cmp_internal(dt1, dt2));
}

Datum
timestamptz_eq_date(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	DateADT		dateVal = PG_GETARG_DATEADT(1);
	TimestampTz dt2;

	dt2 = date2timestamptz(dateVal);

	PG_RETURN_BOOL(timestamptz_cmp_internal(dt1, dt2) == 0);
}

Datum
timestamptz_ne_date(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	DateADT		dateVal = PG_GETARG_DATEADT(1);
	TimestampTz dt2;

	dt2 = date2timestamptz(dateVal);

	PG_RETURN_BOOL(timestamptz_cmp_internal(dt1, dt2) != 0);
}

Datum
timestamptz_lt_date(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	DateADT		dateVal = PG_GETARG_DATEADT(1);
	TimestampTz dt2;

	dt2 = date2timestamptz(dateVal);

	PG_RETURN_BOOL(timestamptz_cmp_internal(dt1, dt2) < 0);
}

Datum
timestamptz_gt_date(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	DateADT		dateVal = PG_GETARG_DATEADT(1);
	TimestampTz dt2;

	dt2 = date2timestamptz(dateVal);

	PG_RETURN_BOOL(timestamptz_cmp_internal(dt1, dt2) > 0);
}

Datum
timestamptz_le_date(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	DateADT		dateVal = PG_GETARG_DATEADT(1);
	TimestampTz dt2;

	dt2 = date2timestamptz(dateVal);

	PG_RETURN_BOOL(timestamptz_cmp_internal(dt1, dt2) <= 0);
}

Datum
timestamptz_ge_date(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	DateADT		dateVal = PG_GETARG_DATEADT(1);
	TimestampTz dt2;

	dt2 = date2timestamptz(dateVal);

	PG_RETURN_BOOL(timestamptz_cmp_internal(dt1, dt2) >= 0);
}

Datum
timestamptz_cmp_date(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	DateADT		dateVal = PG_GETARG_DATEADT(1);
	TimestampTz dt2;

	dt2 = date2timestamptz(dateVal);

	PG_RETURN_INT32(timestamptz_cmp_internal(dt1, dt2));
}


/* Add an interval to a date, giving a new date.
 * Must handle both positive and negative intervals.
 *
 * We implement this by promoting the date to timestamp (without time zone)
 * and then using the timestamp plus interval function.
 */
Datum
date_pl_interval(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	Timestamp	dateStamp;

	dateStamp = date2timestamp(dateVal);

	return DirectFunctionCall2(timestamp_pl_interval,
							   TimestampGetDatum(dateStamp),
							   PointerGetDatum(span));
}

/* Subtract an interval from a date, giving a new date.
 * Must handle both positive and negative intervals.
 *
 * We implement this by promoting the date to timestamp (without time zone)
 * and then using the timestamp minus interval function.
 */
Datum
date_mi_interval(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	Timestamp	dateStamp;

	dateStamp = date2timestamp(dateVal);

	return DirectFunctionCall2(timestamp_mi_interval,
							   TimestampGetDatum(dateStamp),
							   PointerGetDatum(span));
}

/* date_timestamp()
 * Convert date to timestamp data type.
 */
Datum
date_timestamp(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	Timestamp	result;

	result = date2timestamp(dateVal);

	PG_RETURN_TIMESTAMP(result);
}


/* timestamp_date()
 * Convert timestamp to date data type.
 */
Datum
timestamp_date(PG_FUNCTION_ARGS)
{
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(0);
	DateADT		result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_NULL();

	if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	result = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - POSTGRES_EPOCH_JDATE;

	PG_RETURN_DATEADT(result);
}


/* date_timestamptz()
 * Convert date to timestamp with time zone data type.
 */
Datum
date_timestamptz(PG_FUNCTION_ARGS)
{
	DateADT		dateVal = PG_GETARG_DATEADT(0);
	TimestampTz result;

	result = date2timestamptz(dateVal);

	PG_RETURN_TIMESTAMP(result);
}


/* timestamptz_date()
 * Convert timestamp with time zone to date data type.
 */
Datum
timestamptz_date(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMP(0);
	DateADT		result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	int			tz;
	char	   *tzn;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_NULL();

	if (timestamp2tm(timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	result = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - POSTGRES_EPOCH_JDATE;

	PG_RETURN_DATEADT(result);
}


/* abstime_date()
 * Convert abstime to date data type.
 */
Datum
abstime_date(PG_FUNCTION_ARGS)
{
	AbsoluteTime abstime = PG_GETARG_ABSOLUTETIME(0);
	DateADT		result;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;

	switch (abstime)
	{
		case INVALID_ABSTIME:
		case NOSTART_ABSTIME:
		case NOEND_ABSTIME:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				   errmsg("cannot convert reserved abstime value to date")));

			/*
			 * pretend to drop through to make compiler think that result will
			 * be set
			 */

		default:
			abstime2tm(abstime, &tz, tm, NULL);
			result = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - POSTGRES_EPOCH_JDATE;
			break;
	}

	PG_RETURN_DATEADT(result);
}


/* date_text()
 * Convert date to text data type.
 */
Datum
date_text(PG_FUNCTION_ARGS)
{
	/* Input is a Date, but may as well leave it in Datum form */
	Datum		date = PG_GETARG_DATUM(0);
	text	   *result;
	char	   *str;
	int			len;

	str = DatumGetCString(DirectFunctionCall1(date_out, date));

	len = strlen(str) + VARHDRSZ;

	result = palloc(len);

	SET_VARSIZE(result, len);
	memcpy(VARDATA(result), str, (len - VARHDRSZ));

	pfree(str);

	PG_RETURN_TEXT_P(result);
}


/* text_date()
 * Convert text string to date.
 * Text type is not null terminated, so use temporary string
 *	then call the standard input routine.
 */
Datum
text_date(PG_FUNCTION_ARGS)
{
	text	   *str = PG_GETARG_TEXT_P(0);
	int			i;
	char	   *sp,
			   *dp,
				dstr[MAXDATELEN + 1];

	if (VARSIZE(str) - VARHDRSZ > MAXDATELEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
				 errmsg("invalid input syntax for type date: \"%s\"",
						DatumGetCString(DirectFunctionCall1(textout,
													PointerGetDatum(str))))));
	
	sp = VARDATA(str);
	dp = dstr;
	for (i = 0; i < (VARSIZE(str) - VARHDRSZ); i++)
		*dp++ = *sp++;
	*dp = '\0';

	return DirectFunctionCall1(date_in,
							   CStringGetDatum(dstr));
}


/*****************************************************************************
 *	 Time ADT
 *****************************************************************************/

Datum
time_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	TimeADT		result;
	fsec_t		fsec = 0;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;
	int			nf;
	int			dterr;
	char		workbuf[MAXDATELEN + 1];
	char	   *field[MAXDATEFIELDS];
	int			dtype = 0;
	int			ftype[MAXDATEFIELDS];

	dterr = ParseDateTime(str, workbuf, sizeof(workbuf),
						  field, ftype, MAXDATEFIELDS, &nf);
	if (dterr == 0)
		dterr = DecodeTimeOnly(field, ftype, nf, &dtype, tm, &fsec, &tz);
	if (dterr != 0)
		DateTimeParseError(dterr, str, "time");

	tm2time(tm, fsec, &result);
	AdjustTimeForTypmod(&result, typmod);

	PG_RETURN_TIMEADT(result);
}

/* tm2time()
 * Convert a tm structure to a time data type.
 */
static int
tm2time(struct pg_tm * tm, fsec_t fsec, TimeADT *result)
{
#ifdef HAVE_INT64_TIMESTAMP
	*result = ((((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec)
			   * USECS_PER_SEC) + fsec;
#else
	*result = ((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec + fsec;
#endif
	return 0;
}

/* time2tm()
 * Convert time data type to POSIX time structure.
 *
 * For dates within the range of pg_time_t, convert to the local time zone.
 * If out of this range, leave as UTC (in practice that could only happen
 * if pg_time_t is just 32 bits) - thomas 97/05/27
 */
static int
time2tm(TimeADT time, struct pg_tm * tm, fsec_t *fsec)
{
#ifdef HAVE_INT64_TIMESTAMP
	tm->tm_hour = time / USECS_PER_HOUR;
	time -= tm->tm_hour * USECS_PER_HOUR;
	tm->tm_min = time / USECS_PER_MINUTE;
	time -= tm->tm_min * USECS_PER_MINUTE;
	tm->tm_sec = time / USECS_PER_SEC;
	time -= tm->tm_sec * USECS_PER_SEC;
	*fsec = time;
#else
	double		trem;

recalc:
	trem = time;
	TMODULO(trem, tm->tm_hour, (double) SECS_PER_HOUR);
	TMODULO(trem, tm->tm_min, (double) SECS_PER_MINUTE);
	TMODULO(trem, tm->tm_sec, 1.0);
	trem = TIMEROUND(trem);
	/* roundoff may need to propagate to higher-order fields */
	if (trem >= 1.0)
	{
		time = ceil(time);
		goto recalc;
	}
	*fsec = trem;
#endif

	return 0;
}

Datum
time_out(PG_FUNCTION_ARGS)
{
	TimeADT		time = PG_GETARG_TIMEADT(0);
	char	   *result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	char		buf[MAXDATELEN + 1];

	time2tm(time, tm, &fsec);
	EncodeTimeOnly(tm, fsec, NULL, DateStyle, buf);

	result = pstrdup(buf);
	PG_RETURN_CSTRING(result);
}

/*
 *		time_recv			- converts external binary format to time
 *
 * We make no attempt to provide compatibility between int and float
 * time representations ...
 */
Datum
time_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	TimeADT		result;

#ifdef HAVE_INT64_TIMESTAMP
	result = pq_getmsgint64(buf);
#else
	result = pq_getmsgfloat8(buf);
#endif

	AdjustTimeForTypmod(&result, typmod);

	PG_RETURN_TIMEADT(result);
}

/*
 *		time_send			- converts time to binary format
 */
Datum
time_send(PG_FUNCTION_ARGS)
{
	TimeADT		time = PG_GETARG_TIMEADT(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
#ifdef HAVE_INT64_TIMESTAMP
	pq_sendint64(&buf, time);
#else
	pq_sendfloat8(&buf, time);
#endif
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


/* time_scale()
 * Adjust time type for specified scale factor.
 * Used by PostgreSQL type system to stuff columns.
 */
Datum
time_scale(PG_FUNCTION_ARGS)
{
	TimeADT		time = PG_GETARG_TIMEADT(0);
	int32		typmod = PG_GETARG_INT32(1);
	TimeADT		result;

	result = time;
	AdjustTimeForTypmod(&result, typmod);

	PG_RETURN_TIMEADT(result);
}

/* AdjustTimeForTypmod()
 * Force the precision of the time value to a specified value.
 * Uses *exactly* the same code as in AdjustTimestampForTypemod()
 * but we make a separate copy because those types do not
 * have a fundamental tie together but rather a coincidence of
 * implementation. - thomas
 */
static void
AdjustTimeForTypmod(TimeADT *time, int32 typmod)
{
#ifdef HAVE_INT64_TIMESTAMP
	static const int64 TimeScales[MAX_TIME_PRECISION + 1] = {
		INT64CONST(1000000),
		INT64CONST(100000),
		INT64CONST(10000),
		INT64CONST(1000),
		INT64CONST(100),
		INT64CONST(10),
		INT64CONST(1)
	};

	static const int64 TimeOffsets[MAX_TIME_PRECISION + 1] = {
		INT64CONST(500000),
		INT64CONST(50000),
		INT64CONST(5000),
		INT64CONST(500),
		INT64CONST(50),
		INT64CONST(5),
		INT64CONST(0)
	};
#else
	/* note MAX_TIME_PRECISION differs in this case */
	static const double TimeScales[MAX_TIME_PRECISION + 1] = {
		1.0,
		10.0,
		100.0,
		1000.0,
		10000.0,
		100000.0,
		1000000.0,
		10000000.0,
		100000000.0,
		1000000000.0,
		10000000000.0
	};
#endif

	if (typmod >= 0 && typmod <= MAX_TIME_PRECISION)
	{
		/*
		 * Note: this round-to-nearest code is not completely consistent about
		 * rounding values that are exactly halfway between integral values.
		 * On most platforms, rint() will implement round-to-nearest-even, but
		 * the integer code always rounds up (away from zero).	Is it worth
		 * trying to be consistent?
		 */
#ifdef HAVE_INT64_TIMESTAMP
		if (*time >= INT64CONST(0))
			*time = ((*time + TimeOffsets[typmod]) / TimeScales[typmod]) *
				TimeScales[typmod];
		else
			*time = -((((-*time) + TimeOffsets[typmod]) / TimeScales[typmod]) *
					  TimeScales[typmod]);
#else
		*time = rint((double) *time * TimeScales[typmod]) / TimeScales[typmod];
#endif
	}
}


Datum
time_eq(PG_FUNCTION_ARGS)
{
	TimeADT		time1 = PG_GETARG_TIMEADT(0);
	TimeADT		time2 = PG_GETARG_TIMEADT(1);

	PG_RETURN_BOOL(time1 == time2);
}

Datum
time_ne(PG_FUNCTION_ARGS)
{
	TimeADT		time1 = PG_GETARG_TIMEADT(0);
	TimeADT		time2 = PG_GETARG_TIMEADT(1);

	PG_RETURN_BOOL(time1 != time2);
}

Datum
time_lt(PG_FUNCTION_ARGS)
{
	TimeADT		time1 = PG_GETARG_TIMEADT(0);
	TimeADT		time2 = PG_GETARG_TIMEADT(1);

	PG_RETURN_BOOL(time1 < time2);
}

Datum
time_le(PG_FUNCTION_ARGS)
{
	TimeADT		time1 = PG_GETARG_TIMEADT(0);
	TimeADT		time2 = PG_GETARG_TIMEADT(1);

	PG_RETURN_BOOL(time1 <= time2);
}

Datum
time_gt(PG_FUNCTION_ARGS)
{
	TimeADT		time1 = PG_GETARG_TIMEADT(0);
	TimeADT		time2 = PG_GETARG_TIMEADT(1);

	PG_RETURN_BOOL(time1 > time2);
}

Datum
time_ge(PG_FUNCTION_ARGS)
{
	TimeADT		time1 = PG_GETARG_TIMEADT(0);
	TimeADT		time2 = PG_GETARG_TIMEADT(1);

	PG_RETURN_BOOL(time1 >= time2);
}

Datum
time_cmp(PG_FUNCTION_ARGS)
{
	TimeADT		time1 = PG_GETARG_TIMEADT(0);
	TimeADT		time2 = PG_GETARG_TIMEADT(1);

	if (time1 < time2)
		PG_RETURN_INT32(-1);
	if (time1 > time2)
		PG_RETURN_INT32(1);
	PG_RETURN_INT32(0);
}

#if  0
Datum
time_hash(PG_FUNCTION_ARGS)
{
	/* We can use either hashint8 or hashfloat8 directly */
#ifdef HAVE_INT64_TIMESTAMP
	return hashint8(fcinfo);
#else
	return hashfloat8(fcinfo);
#endif
}
#endif

Datum
time_larger(PG_FUNCTION_ARGS)
{
	TimeADT		time1 = PG_GETARG_TIMEADT(0);
	TimeADT		time2 = PG_GETARG_TIMEADT(1);

	PG_RETURN_TIMEADT((time1 > time2) ? time1 : time2);
}

Datum
time_smaller(PG_FUNCTION_ARGS)
{
	TimeADT		time1 = PG_GETARG_TIMEADT(0);
	TimeADT		time2 = PG_GETARG_TIMEADT(1);

	PG_RETURN_TIMEADT((time1 < time2) ? time1 : time2);
}

/* overlaps_time() --- implements the SQL92 OVERLAPS operator.
 *
 * Algorithm is per SQL92 spec.  This is much harder than you'd think
 * because the spec requires us to deliver a non-null answer in some cases
 * where some of the inputs are null.
 */
Datum
overlaps_time(PG_FUNCTION_ARGS)
{
	/*
	 * The arguments are TimeADT, but we leave them as generic Datums to avoid
	 * dereferencing nulls (TimeADT is pass-by-reference!)
	 */
	Datum		ts1 = PG_GETARG_DATUM(0);
	Datum		te1 = PG_GETARG_DATUM(1);
	Datum		ts2 = PG_GETARG_DATUM(2);
	Datum		te2 = PG_GETARG_DATUM(3);
	bool		ts1IsNull = PG_ARGISNULL(0);
	bool		te1IsNull = PG_ARGISNULL(1);
	bool		ts2IsNull = PG_ARGISNULL(2);
	bool		te2IsNull = PG_ARGISNULL(3);

#define TIMEADT_GT(t1,t2) \
	(DatumGetTimeADT(t1) > DatumGetTimeADT(t2))
#define TIMEADT_LT(t1,t2) \
	(DatumGetTimeADT(t1) < DatumGetTimeADT(t2))

	/*
	 * If both endpoints of interval 1 are null, the result is null (unknown).
	 * If just one endpoint is null, take ts1 as the non-null one. Otherwise,
	 * take ts1 as the lesser endpoint.
	 */
	if (ts1IsNull)
	{
		if (te1IsNull)
			PG_RETURN_NULL();
		/* swap null for non-null */
		ts1 = te1;
		te1IsNull = true;
	}
	else if (!te1IsNull)
	{
		if (TIMEADT_GT(ts1, te1))
		{
			Datum		tt = ts1;

			ts1 = te1;
			te1 = tt;
		}
	}

	/* Likewise for interval 2. */
	if (ts2IsNull)
	{
		if (te2IsNull)
			PG_RETURN_NULL();
		/* swap null for non-null */
		ts2 = te2;
		te2IsNull = true;
	}
	else if (!te2IsNull)
	{
		if (TIMEADT_GT(ts2, te2))
		{
			Datum		tt = ts2;

			ts2 = te2;
			te2 = tt;
		}
	}

	/*
	 * At this point neither ts1 nor ts2 is null, so we can consider three
	 * cases: ts1 > ts2, ts1 < ts2, ts1 = ts2
	 */
	if (TIMEADT_GT(ts1, ts2))
	{
		/*
		 * This case is ts1 < te2 OR te1 < te2, which may look redundant but
		 * in the presence of nulls it's not quite completely so.
		 */
		if (te2IsNull)
			PG_RETURN_NULL();
		if (TIMEADT_LT(ts1, te2))
			PG_RETURN_BOOL(true);
		if (te1IsNull)
			PG_RETURN_NULL();

		/*
		 * If te1 is not null then we had ts1 <= te1 above, and we just found
		 * ts1 >= te2, hence te1 >= te2.
		 */
		PG_RETURN_BOOL(false);
	}
	else if (TIMEADT_LT(ts1, ts2))
	{
		/* This case is ts2 < te1 OR te2 < te1 */
		if (te1IsNull)
			PG_RETURN_NULL();
		if (TIMEADT_LT(ts2, te1))
			PG_RETURN_BOOL(true);
		if (te2IsNull)
			PG_RETURN_NULL();

		/*
		 * If te2 is not null then we had ts2 <= te2 above, and we just found
		 * ts2 >= te1, hence te2 >= te1.
		 */
		PG_RETURN_BOOL(false);
	}
	else
	{
		/*
		 * For ts1 = ts2 the spec says te1 <> te2 OR te1 = te2, which is a
		 * rather silly way of saying "true if both are nonnull, else null".
		 */
		if (te1IsNull || te2IsNull)
			PG_RETURN_NULL();
		PG_RETURN_BOOL(true);
	}

#undef TIMEADT_GT
#undef TIMEADT_LT
}

/* timestamp_time()
 * Convert timestamp to time data type.
 */
Datum
timestamp_time(PG_FUNCTION_ARGS)
{
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(0);
	TimeADT		result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_NULL();

	if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP

	/*
	 * Could also do this with time = (timestamp / USECS_PER_DAY *
	 * USECS_PER_DAY) - timestamp;
	 */
	result = ((((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec) *
			  USECS_PER_SEC) + fsec;
#else
	result = ((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec + fsec;
#endif

	PG_RETURN_TIMEADT(result);
}

/* timestamptz_time()
 * Convert timestamptz to time data type.
 */
Datum
timestamptz_time(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMP(0);
	TimeADT		result;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;
	fsec_t		fsec;
	char	   *tzn;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_NULL();

	if (timestamp2tm(timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP

	/*
	 * Could also do this with time = (timestamp / USECS_PER_DAY *
	 * USECS_PER_DAY) - timestamp;
	 */
	result = ((((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec) *
			  USECS_PER_SEC) + fsec;
#else
	result = ((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec + fsec;
#endif

	PG_RETURN_TIMEADT(result);
}

/* datetime_timestamp()
 * Convert date and time to timestamp data type.
 */
Datum
datetime_timestamp(PG_FUNCTION_ARGS)
{
	DateADT		date = PG_GETARG_DATEADT(0);
	TimeADT		time = PG_GETARG_TIMEADT(1);
	Timestamp	result;

	result = DatumGetTimestamp(DirectFunctionCall1(date_timestamp,
												   DateADTGetDatum(date)));
	result += time;

	PG_RETURN_TIMESTAMP(result);
}

/* time_interval()
 * Convert time to interval data type.
 */
Datum
time_interval(PG_FUNCTION_ARGS)
{
	TimeADT		time = PG_GETARG_TIMEADT(0);
	Interval   *result;

	result = (Interval *) palloc(sizeof(Interval));

	result->time = time;
	result->day = 0;
	result->month = 0;

	PG_RETURN_INTERVAL_P(result);
}

/* interval_time()
 * Convert interval to time data type.
 *
 * This is defined as producing the fractional-day portion of the interval.
 * Therefore, we can just ignore the months field.	It is not real clear
 * what to do with negative intervals, but we choose to subtract the floor,
 * so that, say, '-2 hours' becomes '22:00:00'.
 */
Datum
interval_time(PG_FUNCTION_ARGS)
{
	Interval   *span = PG_GETARG_INTERVAL_P(0);
	TimeADT		result;

#ifdef HAVE_INT64_TIMESTAMP
	int64		days;

	result = span->time;
	if (result >= USECS_PER_DAY)
	{
		days = result / USECS_PER_DAY;
		result -= days * USECS_PER_DAY;
	}
	else if (result < 0)
	{
		days = (-result + USECS_PER_DAY - 1) / USECS_PER_DAY;
		result += days * USECS_PER_DAY;
	}
#else
	result = span->time;
	if (result >= (double) SECS_PER_DAY || result < 0)
		result -= floor(result / (double) SECS_PER_DAY) * (double) SECS_PER_DAY;
#endif

	PG_RETURN_TIMEADT(result);
}

/* time_mi_time()
 * Subtract two times to produce an interval.
 */
Datum
time_mi_time(PG_FUNCTION_ARGS)
{
	TimeADT		time1 = PG_GETARG_TIMEADT(0);
	TimeADT		time2 = PG_GETARG_TIMEADT(1);
	Interval   *result;

	result = (Interval *) palloc(sizeof(Interval));

	result->month = 0;
	result->day = 0;
	result->time = time1 - time2;

	PG_RETURN_INTERVAL_P(result);
}

/* time_pl_interval()
 * Add interval to time.
 */
Datum
time_pl_interval(PG_FUNCTION_ARGS)
{
	TimeADT		time = PG_GETARG_TIMEADT(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	TimeADT		result;

#ifdef HAVE_INT64_TIMESTAMP
	result = time + span->time;
	result -= result / USECS_PER_DAY * USECS_PER_DAY;
	if (result < INT64CONST(0))
		result += USECS_PER_DAY;
#else
	TimeADT		time1;

	result = time + span->time;
	TMODULO(result, time1, (double) SECS_PER_DAY);
	if (result < 0)
		result += SECS_PER_DAY;
#endif

	PG_RETURN_TIMEADT(result);
}

/* time_mi_interval()
 * Subtract interval from time.
 */
Datum
time_mi_interval(PG_FUNCTION_ARGS)
{
	TimeADT		time = PG_GETARG_TIMEADT(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	TimeADT		result;

#ifdef HAVE_INT64_TIMESTAMP
	result = time - span->time;
	result -= result / USECS_PER_DAY * USECS_PER_DAY;
	if (result < INT64CONST(0))
		result += USECS_PER_DAY;
#else
	TimeADT		time1;

	result = time - span->time;
	TMODULO(result, time1, (double) SECS_PER_DAY);
	if (result < 0)
		result += SECS_PER_DAY;
#endif

	PG_RETURN_TIMEADT(result);
}


/* time_text()
 * Convert time to text data type.
 */
Datum
time_text(PG_FUNCTION_ARGS)
{
	/* Input is a Time, but may as well leave it in Datum form */
	Datum		time = PG_GETARG_DATUM(0);
	text	   *result;
	char	   *str;
	int			len;

	str = DatumGetCString(DirectFunctionCall1(time_out, time));

	len = strlen(str) + VARHDRSZ;

	result = palloc(len);

	SET_VARSIZE(result, len);
	memcpy(VARDATA(result), str, (len - VARHDRSZ));

	pfree(str);

	PG_RETURN_TEXT_P(result);
}


/* text_time()
 * Convert text string to time.
 * Text type is not null terminated, so use temporary string
 *	then call the standard input routine.
 */
Datum
text_time(PG_FUNCTION_ARGS)
{
	text	   *str = PG_GETARG_TEXT_P(0);
	int			i;
	char	   *sp,
			   *dp,
				dstr[MAXDATELEN + 1];

	if (VARSIZE(str) - VARHDRSZ > MAXDATELEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
				 errmsg("invalid input syntax for type time: \"%s\"",
						DatumGetCString(DirectFunctionCall1(textout, 
													PointerGetDatum(str))))));

	sp = VARDATA(str);
	dp = dstr;
	for (i = 0; i < (VARSIZE(str) - VARHDRSZ); i++)
		*dp++ = *sp++;
	*dp = '\0';

	return DirectFunctionCall3(time_in,
							   CStringGetDatum(dstr),
							   ObjectIdGetDatum(InvalidOid),
							   Int32GetDatum(-1));
}

/* time_part()
 * Extract specified field from time type.
 */
Datum
time_part(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_P(0);
	TimeADT		time = PG_GETARG_TIMEADT(1);
	float8		result;
	int			type,
				val;
	char	   *lowunits;

	lowunits = downcase_truncate_identifier(VARDATA(units),
											VARSIZE(units) - VARHDRSZ,
											false);

	type = DecodeUnits(0, lowunits, &val);
	if (type == UNKNOWN_FIELD)
		type = DecodeSpecial(0, lowunits, &val);

	if (type == UNITS)
	{
		fsec_t		fsec;
		struct pg_tm tt,
				   *tm = &tt;

		time2tm(time, tm, &fsec);

		switch (val)
		{
			case DTK_MICROSEC:
#ifdef HAVE_INT64_TIMESTAMP
				result = tm->tm_sec * USECS_PER_SEC + fsec;
#else
				result = (tm->tm_sec + fsec) * 1000000;
#endif
				break;

			case DTK_MILLISEC:
#ifdef HAVE_INT64_TIMESTAMP
				result = tm->tm_sec * INT64CONST(1000) + fsec / INT64CONST(1000);
#else
				result = (tm->tm_sec + fsec) * 1000;
#endif
				break;

			case DTK_SECOND:
#ifdef HAVE_INT64_TIMESTAMP
				result = tm->tm_sec + fsec / USECS_PER_SEC;
#else
				result = tm->tm_sec + fsec;
#endif
				break;

			case DTK_MINUTE:
				result = tm->tm_min;
				break;

			case DTK_HOUR:
				result = tm->tm_hour;
				break;

			case DTK_TZ:
			case DTK_TZ_MINUTE:
			case DTK_TZ_HOUR:
			case DTK_DAY:
			case DTK_MONTH:
			case DTK_QUARTER:
			case DTK_YEAR:
			case DTK_DECADE:
			case DTK_CENTURY:
			case DTK_MILLENNIUM:
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("\"time\" units \"%s\" not recognized",
								DatumGetCString(DirectFunctionCall1(textout,
												 PointerGetDatum(units))))));

				result = 0;
		}
	}
	else if (type == RESERV && val == DTK_EPOCH)
	{
#ifdef HAVE_INT64_TIMESTAMP
		result = time / 1000000.0;
#else
		result = time;
#endif
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"time\" units \"%s\" not recognized",
						DatumGetCString(DirectFunctionCall1(textout,
												 PointerGetDatum(units))))));
		result = 0;
	}

	PG_RETURN_FLOAT8(result);
}


/*****************************************************************************
 *	 Time With Time Zone ADT
 *****************************************************************************/

/* tm2timetz()
 * Convert a tm structure to a time data type.
 */
static int
tm2timetz(struct pg_tm * tm, fsec_t fsec, int tz, TimeTzADT *result)
{
#ifdef HAVE_INT64_TIMESTAMP
	result->time = ((((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec) *
					USECS_PER_SEC) + fsec;
#else
	result->time = ((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec + fsec;
#endif
	result->zone = tz;

	return 0;
}

Datum
timetz_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	TimeTzADT  *result;
	fsec_t		fsec = 0;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz = 0;
	int			nf;
	int			dterr;
	char		workbuf[MAXDATELEN + 1];
	char	   *field[MAXDATEFIELDS];
	int			dtype = 0;
	int			ftype[MAXDATEFIELDS];

	dterr = ParseDateTime(str, workbuf, sizeof(workbuf),
						  field, ftype, MAXDATEFIELDS, &nf);
	if (dterr == 0)
		dterr = DecodeTimeOnly(field, ftype, nf, &dtype, tm, &fsec, &tz);
	if (dterr != 0)
		DateTimeParseError(dterr, str, "time with time zone");

	result = (TimeTzADT *) palloc(sizeof(TimeTzADT));
	tm2timetz(tm, fsec, tz, result);
	AdjustTimeForTypmod(&(result->time), typmod);

	PG_RETURN_TIMETZADT_P(result);
}

Datum
timetz_out(PG_FUNCTION_ARGS)
{
	TimeTzADT  *time = PG_GETARG_TIMETZADT_P(0);
	char	   *result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	int			tz;
	char		buf[MAXDATELEN + 1];

	timetz2tm(time, tm, &fsec, &tz);
	EncodeTimeOnly(tm, fsec, &tz, DateStyle, buf);

	result = pstrdup(buf);
	PG_RETURN_CSTRING(result);
}

/*
 *		timetz_recv			- converts external binary format to timetz
 */
Datum
timetz_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	TimeTzADT  *result;

	result = (TimeTzADT *) palloc(sizeof(TimeTzADT));

#ifdef HAVE_INT64_TIMESTAMP
	result->time = pq_getmsgint64(buf);
#else
	result->time = pq_getmsgfloat8(buf);
#endif
	result->zone = pq_getmsgint(buf, sizeof(result->zone));

	AdjustTimeForTypmod(&(result->time), typmod);

	PG_RETURN_TIMETZADT_P(result);
}

/*
 *		timetz_send			- converts timetz to binary format
 */
Datum
timetz_send(PG_FUNCTION_ARGS)
{
	TimeTzADT  *time = PG_GETARG_TIMETZADT_P(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
#ifdef HAVE_INT64_TIMESTAMP
	pq_sendint64(&buf, time->time);
#else
	pq_sendfloat8(&buf, time->time);
#endif
	pq_sendint(&buf, time->zone, sizeof(time->zone));
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


/* timetz2tm()
 * Convert TIME WITH TIME ZONE data type to POSIX time structure.
 */
static int
timetz2tm(TimeTzADT *time, struct pg_tm * tm, fsec_t *fsec, int *tzp)
{
#ifdef HAVE_INT64_TIMESTAMP
	int64		trem = time->time;

	tm->tm_hour = trem / USECS_PER_HOUR;
	trem -= tm->tm_hour * USECS_PER_HOUR;
	tm->tm_min = trem / USECS_PER_MINUTE;
	trem -= tm->tm_min * USECS_PER_MINUTE;
	tm->tm_sec = trem / USECS_PER_SEC;
	*fsec = trem - tm->tm_sec * USECS_PER_SEC;
#else
	double		trem = time->time;

recalc:
	TMODULO(trem, tm->tm_hour, (double) SECS_PER_HOUR);
	TMODULO(trem, tm->tm_min, (double) SECS_PER_MINUTE);
	TMODULO(trem, tm->tm_sec, 1.0);
	trem = TIMEROUND(trem);
	/* roundoff may need to propagate to higher-order fields */
	if (trem >= 1.0)
	{
		trem = ceil(time->time);
		goto recalc;
	}
	*fsec = trem;
#endif

	if (tzp != NULL)
		*tzp = time->zone;

	return 0;
}

/* timetz_scale()
 * Adjust time type for specified scale factor.
 * Used by PostgreSQL type system to stuff columns.
 */
Datum
timetz_scale(PG_FUNCTION_ARGS)
{
	TimeTzADT  *time = PG_GETARG_TIMETZADT_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	TimeTzADT  *result;

	result = (TimeTzADT *) palloc(sizeof(TimeTzADT));

	result->time = time->time;
	result->zone = time->zone;

	AdjustTimeForTypmod(&(result->time), typmod);

	PG_RETURN_TIMETZADT_P(result);
}


static int
timetz_cmp_internal(TimeTzADT *time1, TimeTzADT *time2)
{
	/* Primary sort is by true (GMT-equivalent) time */
#ifdef HAVE_INT64_TIMESTAMP
	int64		t1,
				t2;

	t1 = time1->time + (time1->zone * USECS_PER_SEC);
	t2 = time2->time + (time2->zone * USECS_PER_SEC);
#else
	double		t1,
				t2;

	t1 = time1->time + time1->zone;
	t2 = time2->time + time2->zone;
#endif

	if (t1 > t2)
		return 1;
	if (t1 < t2)
		return -1;

	/*
	 * If same GMT time, sort by timezone; we only want to say that two
	 * timetz's are equal if both the time and zone parts are equal.
	 */
	if (time1->zone > time2->zone)
		return 1;
	if (time1->zone < time2->zone)
		return -1;

	return 0;
}

Datum
timetz_eq(PG_FUNCTION_ARGS)
{
	TimeTzADT  *time1 = PG_GETARG_TIMETZADT_P(0);
	TimeTzADT  *time2 = PG_GETARG_TIMETZADT_P(1);

	PG_RETURN_BOOL(timetz_cmp_internal(time1, time2) == 0);
}

Datum
timetz_ne(PG_FUNCTION_ARGS)
{
	TimeTzADT  *time1 = PG_GETARG_TIMETZADT_P(0);
	TimeTzADT  *time2 = PG_GETARG_TIMETZADT_P(1);

	PG_RETURN_BOOL(timetz_cmp_internal(time1, time2) != 0);
}

Datum
timetz_lt(PG_FUNCTION_ARGS)
{
	TimeTzADT  *time1 = PG_GETARG_TIMETZADT_P(0);
	TimeTzADT  *time2 = PG_GETARG_TIMETZADT_P(1);

	PG_RETURN_BOOL(timetz_cmp_internal(time1, time2) < 0);
}

Datum
timetz_le(PG_FUNCTION_ARGS)
{
	TimeTzADT  *time1 = PG_GETARG_TIMETZADT_P(0);
	TimeTzADT  *time2 = PG_GETARG_TIMETZADT_P(1);

	PG_RETURN_BOOL(timetz_cmp_internal(time1, time2) <= 0);
}

Datum
timetz_gt(PG_FUNCTION_ARGS)
{
	TimeTzADT  *time1 = PG_GETARG_TIMETZADT_P(0);
	TimeTzADT  *time2 = PG_GETARG_TIMETZADT_P(1);

	PG_RETURN_BOOL(timetz_cmp_internal(time1, time2) > 0);
}

Datum
timetz_ge(PG_FUNCTION_ARGS)
{
	TimeTzADT  *time1 = PG_GETARG_TIMETZADT_P(0);
	TimeTzADT  *time2 = PG_GETARG_TIMETZADT_P(1);

	PG_RETURN_BOOL(timetz_cmp_internal(time1, time2) >= 0);
}

Datum
timetz_cmp(PG_FUNCTION_ARGS)
{
	TimeTzADT  *time1 = PG_GETARG_TIMETZADT_P(0);
	TimeTzADT  *time2 = PG_GETARG_TIMETZADT_P(1);

	PG_RETURN_INT32(timetz_cmp_internal(time1, time2));
}

/*
 * timetz, being an unusual size, needs a specialized hash function.
 */
Datum
timetz_hash(PG_FUNCTION_ARGS)
{
	TimeTzADT  *key = PG_GETARG_TIMETZADT_P(0);

	/*
	 * Specify hash length as sizeof(double) + sizeof(int4), not as
	 * sizeof(TimeTzADT), so that any garbage pad bytes in the structure won't
	 * be included in the hash!
	 */
	return hash_any((unsigned char *) key, sizeof(key->time) + sizeof(key->zone));
}

Datum
timetz_larger(PG_FUNCTION_ARGS)
{
	TimeTzADT  *time1 = PG_GETARG_TIMETZADT_P(0);
	TimeTzADT  *time2 = PG_GETARG_TIMETZADT_P(1);
	TimeTzADT  *result;

	if (timetz_cmp_internal(time1, time2) > 0)
		result = time1;
	else
		result = time2;
	PG_RETURN_TIMETZADT_P(result);
}

Datum
timetz_smaller(PG_FUNCTION_ARGS)
{
	TimeTzADT  *time1 = PG_GETARG_TIMETZADT_P(0);
	TimeTzADT  *time2 = PG_GETARG_TIMETZADT_P(1);
	TimeTzADT  *result;

	if (timetz_cmp_internal(time1, time2) < 0)
		result = time1;
	else
		result = time2;
	PG_RETURN_TIMETZADT_P(result);
}

/* timetz_pl_interval()
 * Add interval to timetz.
 */
Datum
timetz_pl_interval(PG_FUNCTION_ARGS)
{
	TimeTzADT  *time = PG_GETARG_TIMETZADT_P(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	TimeTzADT  *result;

#ifndef HAVE_INT64_TIMESTAMP
	TimeTzADT	time1;
#endif

	result = (TimeTzADT *) palloc(sizeof(TimeTzADT));

#ifdef HAVE_INT64_TIMESTAMP
	result->time = time->time + span->time;
	result->time -= result->time / USECS_PER_DAY * USECS_PER_DAY;
	if (result->time < INT64CONST(0))
		result->time += USECS_PER_DAY;
#else
	result->time = time->time + span->time;
	TMODULO(result->time, time1.time, (double) SECS_PER_DAY);
	if (result->time < 0)
		result->time += SECS_PER_DAY;
#endif

	result->zone = time->zone;

	PG_RETURN_TIMETZADT_P(result);
}

/* timetz_mi_interval()
 * Subtract interval from timetz.
 */
Datum
timetz_mi_interval(PG_FUNCTION_ARGS)
{
	TimeTzADT  *time = PG_GETARG_TIMETZADT_P(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	TimeTzADT  *result;

#ifndef HAVE_INT64_TIMESTAMP
	TimeTzADT	time1;
#endif

	result = (TimeTzADT *) palloc(sizeof(TimeTzADT));

#ifdef HAVE_INT64_TIMESTAMP
	result->time = time->time - span->time;
	result->time -= result->time / USECS_PER_DAY * USECS_PER_DAY;
	if (result->time < INT64CONST(0))
		result->time += USECS_PER_DAY;
#else
	result->time = time->time - span->time;
	TMODULO(result->time, time1.time, (double) SECS_PER_DAY);
	if (result->time < 0)
		result->time += SECS_PER_DAY;
#endif

	result->zone = time->zone;

	PG_RETURN_TIMETZADT_P(result);
}

/* overlaps_timetz() --- implements the SQL92 OVERLAPS operator.
 *
 * Algorithm is per SQL92 spec.  This is much harder than you'd think
 * because the spec requires us to deliver a non-null answer in some cases
 * where some of the inputs are null.
 */
Datum
overlaps_timetz(PG_FUNCTION_ARGS)
{
	/*
	 * The arguments are TimeTzADT *, but we leave them as generic Datums for
	 * convenience of notation --- and to avoid dereferencing nulls.
	 */
	Datum		ts1 = PG_GETARG_DATUM(0);
	Datum		te1 = PG_GETARG_DATUM(1);
	Datum		ts2 = PG_GETARG_DATUM(2);
	Datum		te2 = PG_GETARG_DATUM(3);
	bool		ts1IsNull = PG_ARGISNULL(0);
	bool		te1IsNull = PG_ARGISNULL(1);
	bool		ts2IsNull = PG_ARGISNULL(2);
	bool		te2IsNull = PG_ARGISNULL(3);

#define TIMETZ_GT(t1,t2) \
	DatumGetBool(DirectFunctionCall2(timetz_gt,t1,t2))
#define TIMETZ_LT(t1,t2) \
	DatumGetBool(DirectFunctionCall2(timetz_lt,t1,t2))

	/*
	 * If both endpoints of interval 1 are null, the result is null (unknown).
	 * If just one endpoint is null, take ts1 as the non-null one. Otherwise,
	 * take ts1 as the lesser endpoint.
	 */
	if (ts1IsNull)
	{
		if (te1IsNull)
			PG_RETURN_NULL();
		/* swap null for non-null */
		ts1 = te1;
		te1IsNull = true;
	}
	else if (!te1IsNull)
	{
		if (TIMETZ_GT(ts1, te1))
		{
			Datum		tt = ts1;

			ts1 = te1;
			te1 = tt;
		}
	}

	/* Likewise for interval 2. */
	if (ts2IsNull)
	{
		if (te2IsNull)
			PG_RETURN_NULL();
		/* swap null for non-null */
		ts2 = te2;
		te2IsNull = true;
	}
	else if (!te2IsNull)
	{
		if (TIMETZ_GT(ts2, te2))
		{
			Datum		tt = ts2;

			ts2 = te2;
			te2 = tt;
		}
	}

	/*
	 * At this point neither ts1 nor ts2 is null, so we can consider three
	 * cases: ts1 > ts2, ts1 < ts2, ts1 = ts2
	 */
	if (TIMETZ_GT(ts1, ts2))
	{
		/*
		 * This case is ts1 < te2 OR te1 < te2, which may look redundant but
		 * in the presence of nulls it's not quite completely so.
		 */
		if (te2IsNull)
			PG_RETURN_NULL();
		if (TIMETZ_LT(ts1, te2))
			PG_RETURN_BOOL(true);
		if (te1IsNull)
			PG_RETURN_NULL();

		/*
		 * If te1 is not null then we had ts1 <= te1 above, and we just found
		 * ts1 >= te2, hence te1 >= te2.
		 */
		PG_RETURN_BOOL(false);
	}
	else if (TIMETZ_LT(ts1, ts2))
	{
		/* This case is ts2 < te1 OR te2 < te1 */
		if (te1IsNull)
			PG_RETURN_NULL();
		if (TIMETZ_LT(ts2, te1))
			PG_RETURN_BOOL(true);
		if (te2IsNull)
			PG_RETURN_NULL();

		/*
		 * If te2 is not null then we had ts2 <= te2 above, and we just found
		 * ts2 >= te1, hence te2 >= te1.
		 */
		PG_RETURN_BOOL(false);
	}
	else
	{
		/*
		 * For ts1 = ts2 the spec says te1 <> te2 OR te1 = te2, which is a
		 * rather silly way of saying "true if both are nonnull, else null".
		 */
		if (te1IsNull || te2IsNull)
			PG_RETURN_NULL();
		PG_RETURN_BOOL(true);
	}

#undef TIMETZ_GT
#undef TIMETZ_LT
}


Datum
timetz_time(PG_FUNCTION_ARGS)
{
	TimeTzADT  *timetz = PG_GETARG_TIMETZADT_P(0);
	TimeADT		result;

	/* swallow the time zone and just return the time */
	result = timetz->time;

	PG_RETURN_TIMEADT(result);
}


Datum
time_timetz(PG_FUNCTION_ARGS)
{
	TimeADT		time = PG_GETARG_TIMEADT(0);
	TimeTzADT  *result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	int			tz;

	GetCurrentDateTime(tm);
	time2tm(time, tm, &fsec);
	tz = DetermineTimeZoneOffset(tm, session_timezone);

	result = (TimeTzADT *) palloc(sizeof(TimeTzADT));

	result->time = time;
	result->zone = tz;

	PG_RETURN_TIMETZADT_P(result);
}


/* timestamptz_timetz()
 * Convert timestamp to timetz data type.
 */
Datum
timestamptz_timetz(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMP(0);
	TimeTzADT  *result;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;
	fsec_t		fsec;
	char	   *tzn;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_NULL();

	if (timestamp2tm(timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	result = (TimeTzADT *) palloc(sizeof(TimeTzADT));

	tm2timetz(tm, fsec, tz, result);

	PG_RETURN_TIMETZADT_P(result);
}


/* datetimetz_timestamptz()
 * Convert date and timetz to timestamp with time zone data type.
 * Timestamp is stored in GMT, so add the time zone
 * stored with the timetz to the result.
 * - thomas 2000-03-10
 */
Datum
datetimetz_timestamptz(PG_FUNCTION_ARGS)
{
	DateADT		date = PG_GETARG_DATEADT(0);
	TimeTzADT  *time = PG_GETARG_TIMETZADT_P(1);
	TimestampTz result;

#ifdef HAVE_INT64_TIMESTAMP
	result = date * USECS_PER_DAY + time->time + time->zone * USECS_PER_SEC;
#else
	result = date * (double) SECS_PER_DAY + time->time + time->zone;
#endif

	PG_RETURN_TIMESTAMP(result);
}


/* timetz_text()
 * Convert timetz to text data type.
 */
Datum
timetz_text(PG_FUNCTION_ARGS)
{
	/* Input is a Timetz, but may as well leave it in Datum form */
	Datum		timetz = PG_GETARG_DATUM(0);
	text	   *result;
	char	   *str;
	int			len;

	str = DatumGetCString(DirectFunctionCall1(timetz_out, timetz));

	len = strlen(str) + VARHDRSZ;

	result = palloc(len);

	SET_VARSIZE(result, len);
	memcpy(VARDATA(result), str, (len - VARHDRSZ));

	pfree(str);

	PG_RETURN_TEXT_P(result);
}


/* text_timetz()
 * Convert text string to timetz.
 * Text type is not null terminated, so use temporary string
 *	then call the standard input routine.
 */
Datum
text_timetz(PG_FUNCTION_ARGS)
{
	text	   *str = PG_GETARG_TEXT_P(0);
	int			i;
	char	   *sp,
			   *dp,
				dstr[MAXDATELEN + 1];

	if (VARSIZE(str) - VARHDRSZ > MAXDATELEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
		  errmsg("invalid input syntax for type time with time zone: \"%s\"",
				 DatumGetCString(DirectFunctionCall1(textout, 
													 PointerGetDatum(str))))));

	sp = VARDATA(str);
	dp = dstr;
	for (i = 0; i < (VARSIZE(str) - VARHDRSZ); i++)
		*dp++ = *sp++;
	*dp = '\0';

	return DirectFunctionCall3(timetz_in,
							   CStringGetDatum(dstr),
							   ObjectIdGetDatum(InvalidOid),
							   Int32GetDatum(-1));
}

/* timetz_part()
 * Extract specified field from time type.
 */
Datum
timetz_part(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_P(0);
	TimeTzADT  *time = PG_GETARG_TIMETZADT_P(1);
	float8		result;
	int			type,
				val;
	char	   *lowunits;

	lowunits = downcase_truncate_identifier(VARDATA(units),
											VARSIZE(units) - VARHDRSZ,
											false);

	type = DecodeUnits(0, lowunits, &val);
	if (type == UNKNOWN_FIELD)
		type = DecodeSpecial(0, lowunits, &val);

	if (type == UNITS)
	{
		double		dummy;
		int			tz;
		fsec_t		fsec;
		struct pg_tm tt,
				   *tm = &tt;

		timetz2tm(time, tm, &fsec, &tz);

		switch (val)
		{
			case DTK_TZ:
				result = -tz;
				break;

			case DTK_TZ_MINUTE:
				result = -tz;
				result /= SECS_PER_MINUTE;
				FMODULO(result, dummy, (double) SECS_PER_MINUTE);
				break;

			case DTK_TZ_HOUR:
				dummy = -tz;
				FMODULO(dummy, result, (double) SECS_PER_HOUR);
				break;

			case DTK_MICROSEC:
#ifdef HAVE_INT64_TIMESTAMP
				result = tm->tm_sec * USECS_PER_SEC + fsec;
#else
				result = (tm->tm_sec + fsec) * 1000000;
#endif
				break;

			case DTK_MILLISEC:
#ifdef HAVE_INT64_TIMESTAMP
				result = tm->tm_sec * INT64CONST(1000) + fsec / INT64CONST(1000);
#else
				result = (tm->tm_sec + fsec) * 1000;
#endif
				break;

			case DTK_SECOND:
#ifdef HAVE_INT64_TIMESTAMP
				result = tm->tm_sec + fsec / USECS_PER_SEC;
#else
				result = tm->tm_sec + fsec;
#endif
				break;

			case DTK_MINUTE:
				result = tm->tm_min;
				break;

			case DTK_HOUR:
				result = tm->tm_hour;
				break;

			case DTK_DAY:
			case DTK_MONTH:
			case DTK_QUARTER:
			case DTK_YEAR:
			case DTK_DECADE:
			case DTK_CENTURY:
			case DTK_MILLENNIUM:
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("\"time with time zone\" units \"%s\" not recognized",
					   DatumGetCString(DirectFunctionCall1(textout,
												 PointerGetDatum(units))))));

				result = 0;
		}
	}
	else if (type == RESERV && val == DTK_EPOCH)
	{
#ifdef HAVE_INT64_TIMESTAMP
		result = time->time / 1000000.0 + time->zone;
#else
		result = time->time + time->zone;
#endif
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"time with time zone\" units \"%s\" not recognized",
						DatumGetCString(DirectFunctionCall1(textout,
												 PointerGetDatum(units))))));

		result = 0;
	}

	PG_RETURN_FLOAT8(result);
}

/* timetz_zone()
 * Encode time with time zone type with specified time zone.
 * Applies DST rules as of the current date.
 */
Datum
timetz_zone(PG_FUNCTION_ARGS)
{
	text	   *zone = PG_GETARG_TEXT_P(0);
	TimeTzADT  *t = PG_GETARG_TIMETZADT_P(1);
	TimeTzADT  *result;
	int			tz;
	char		tzname[TZ_STRLEN_MAX + 1];
	int			len;
	char	   *lowzone;
	int			type,
				val;
	pg_tz	   *tzp;

	/*
	 * Look up the requested timezone.  First we look in the date token table
	 * (to handle cases like "EST"), and if that fails, we look in the
	 * timezone database (to handle cases like "America/New_York").  (This
	 * matches the order in which timestamp input checks the cases; it's
	 * important because the timezone database unwisely uses a few zone names
	 * that are identical to offset abbreviations.)
	 */
	lowzone = downcase_truncate_identifier(VARDATA(zone),
										   VARSIZE(zone) - VARHDRSZ,
										   false);
	type = DecodeSpecial(0, lowzone, &val);

	if (type == TZ || type == DTZ)
		tz = val * 60;
	else
	{
		len = Min(VARSIZE(zone) - VARHDRSZ, TZ_STRLEN_MAX);
		memcpy(tzname, VARDATA(zone), len);
		tzname[len] = '\0';
		tzp = pg_tzset(tzname);
		if (tzp)
		{
			/* Get the offset-from-GMT that is valid today for the zone */
			pg_time_t	now = (pg_time_t) time(NULL);
			struct pg_tm *tm;

			tm = pg_localtime(&now, tzp);
			tz = -tm->tm_gmtoff;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("time zone \"%s\" not recognized", tzname)));
			tz = 0;				/* keep compiler quiet */
		}
	}

	result = (TimeTzADT *) palloc(sizeof(TimeTzADT));

#ifdef HAVE_INT64_TIMESTAMP
	result->time = t->time + (t->zone - tz) * USECS_PER_SEC;
	while (result->time < INT64CONST(0))
		result->time += USECS_PER_DAY;
	while (result->time >= USECS_PER_DAY)
		result->time -= USECS_PER_DAY;
#else
	result->time = t->time + (t->zone - tz);
	while (result->time < 0)
		result->time += SECS_PER_DAY;
	while (result->time >= SECS_PER_DAY)
		result->time -= SECS_PER_DAY;
#endif

	result->zone = tz;

	PG_RETURN_TIMETZADT_P(result);
}

/* timetz_izone()
 * Encode time with time zone type with specified time interval as time zone.
 */
Datum
timetz_izone(PG_FUNCTION_ARGS)
{
	Interval   *zone = PG_GETARG_INTERVAL_P(0);
	TimeTzADT  *time = PG_GETARG_TIMETZADT_P(1);
	TimeTzADT  *result;
	int			tz;

	if (zone->month != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"interval\" time zone \"%s\" not valid",
						DatumGetCString(DirectFunctionCall1(interval_out,
												  PointerGetDatum(zone))))));

#ifdef HAVE_INT64_TIMESTAMP
	tz = -(zone->time / USECS_PER_SEC);
#else
	tz = -(zone->time);
#endif

	result = (TimeTzADT *) palloc(sizeof(TimeTzADT));

#ifdef HAVE_INT64_TIMESTAMP
	result->time = time->time + (time->zone - tz) * USECS_PER_SEC;
	while (result->time < INT64CONST(0))
		result->time += USECS_PER_DAY;
	while (result->time >= USECS_PER_DAY)
		result->time -= USECS_PER_DAY;
#else
	result->time = time->time + (time->zone - tz);
	while (result->time < 0)
		result->time += SECS_PER_DAY;
	while (result->time >= SECS_PER_DAY)
		result->time -= SECS_PER_DAY;
#endif

	result->zone = tz;

	PG_RETURN_TIMETZADT_P(result);
}

/*-------------------------------------------------------------------------
 *
 * numutils.c
 *	  utility functions for I/O of built-in numeric types.
 *
 *		integer:				pg_atoi, pg_itoa, pg_ltoa
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/utils/adt/numutils.c,v 1.77 2009/01/01 17:23:49 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include <limits.h>
#include <ctype.h>

#include "utils/builtins.h"

/*
 * pg_atoi: convert string to integer
 *
 * allows any number of leading or trailing whitespace characters.
 *
 * 'size' is the sizeof() the desired integral result (1, 2, or 4 bytes).
 *
 * c, if not 0, is a terminator character that may appear after the
 * integer (plus whitespace).  If 0, the string must end after the integer.
 *
 * Unlike plain atoi(), this will throw ereport() upon bad input format or
 * overflow.
 */
int32
pg_atoi(char *s, int size, int c)
{
	long		l;
	char	   *badp;

	/*
	 * Some versions of strtol treat the empty string as an error, but some
	 * seem not to.  Make an explicit test to be sure we catch it.
	 */
	if (s == NULL)
		elog(ERROR, "NULL pointer");
	if (*s == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for integer: \"%s\"",s),
				 errOmitLocation(true)));

	errno = 0;
	l = strtol(s, &badp, 10);

	/* We made no progress parsing the string, so bail out */
	if (s == badp)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for integer: \"%s\"",s),
				 errOmitLocation(true)));

	switch (size)
	{
		case sizeof(int32):
			if (errno == ERANGE
#if defined(HAVE_LONG_INT_64)
			/* won't get ERANGE on these with 64-bit longs... */
				|| l < INT_MIN || l > INT_MAX
#endif
				)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				errmsg("value \"%s\" is out of range for type integer", s),
				errOmitLocation(true)));
			break;
		case sizeof(int16):
			if (errno == ERANGE || l < SHRT_MIN || l > SHRT_MAX)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				errmsg("value \"%s\" is out of range for type smallint", s),
				errOmitLocation(true)));
			break;
		case sizeof(int8):
			if (errno == ERANGE || l < SCHAR_MIN || l > SCHAR_MAX)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				errmsg("value \"%s\" is out of range for 8-bit integer", s),
				errOmitLocation(true)));
			break;
		default:
			elog(ERROR, "unsupported result size: %d", size);
	}

	/*
	 * Skip any trailing whitespace; if anything but whitespace remains before
	 * the terminating character, bail out
	 */
	while (*badp && *badp != c && isspace((unsigned char) *badp))
		badp++;

	if (*badp && *badp != c)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for integer: \"%s\"",s),
				 errOmitLocation(true)));

	return (int32) l;
}

/*
 *		pg_itoa: converts a signed 16-bit integer to its string representation
 *
 *		Caller must ensure that 'a' points to enough memory to hold the result
 *		(at least 7 bytes, counting a leading sign and trailing NUL).
 *
 *		It doesn't seem worth implementing this separately.
 */
void
pg_itoa(int16 i, char *a)
{
		pg_ltoa((int32)i, a);
}

/*
 *		pg_ltoa: converts a signed 32-bit integer to its string representation
 *
 *		Caller must ensure that 'a' points to enough memory to hold the result
 *		(at least 12 bytes, counting a leading sign and trailing NUL).
 *
 *		This is ported from Postgres commit #4fc115b. The previous implementation
 *		in HAWQ allocated a 33 byte char[] when converting but an int32's string
 *		representation requires only a maximum 12.
 */
void
pg_ltoa(int32 l, char *a)
{
	char *start = a;
	bool neg = false;

	/*
	 * Avoid problems with the most negative integer not being representable
	 * as a positive integer.
	 */
	if (l == INT32_MIN)
	{
		memcpy(a, "-2147483648", 12);
		return;
	}
	else if (l < 0)
	{
		l = -l;
		neg = true;
	}

	/* Compute the result backwards. */
	do
	{
		int32 remainder;
		int32 oldval = l;
		l /= 10;
		remainder = oldval - l * 10;
		*a++  = '0' + remainder;
	} while (l != 0);
	if (neg)
		*a++ = '-';

	/* Add trailing NUL byte. */
	*a-- = '\0';

	/* reverse string */
	while (start < a)
	{
		char swap = *start;
		*start++ = *a;
		*a-- = swap;
	}

}

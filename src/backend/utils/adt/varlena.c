/*-------------------------------------------------------------------------
 *
 * varlena.c
 *	  Functions for the variable-length built-in types.
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/utils/adt/varlena.c,v 1.154 2007/01/05 22:19:42 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>

#include "access/tupmacs.h"
#include "access/tuptoaster.h"
#include "catalog/pg_type.h"
#include "libpq/md5.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/pg_locale.h"
#include "utils/string_wrapper.h"
#include "utils/memutils.h"

typedef struct varlena unknown;

typedef struct
{
	bool		use_wchar;		/* T if multibyte encoding */
	char	   *str1;			/* use these if not use_wchar */
	char	   *str2;			/* note: these point to original texts */
	pg_wchar   *wstr1;			/* use these if use_wchar */
	pg_wchar   *wstr2;			/* note: these are palloc'd */
	int			len1;			/* string lengths in logical characters */
	int			len2;
} TextPositionState;

#define DatumGetUnknownP(X)			((unknown *) PG_DETOAST_DATUM(X))
#define DatumGetUnknownPCopy(X)		((unknown *) PG_DETOAST_DATUM_COPY(X))
#define PG_GETARG_UNKNOWN_P(n)		DatumGetUnknownP(PG_GETARG_DATUM(n))
#define PG_GETARG_UNKNOWN_P_COPY(n) DatumGetUnknownPCopy(PG_GETARG_DATUM(n))
#define PG_RETURN_UNKNOWN_P(x)		PG_RETURN_POINTER(x)

#define PG_TEXTARG_GET_STR(arg_) \
	DatumGetCString(DirectFunctionCall1(textout, PG_GETARG_DATUM(arg_)))
#define PG_TEXT_GET_STR(textp_) \
	DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp_)))
#define PG_STR_GET_TEXT(str_) \
	DatumGetTextP(DirectFunctionCall1(textin, CStringGetDatum(str_)))

/*
 * Max considered sub-string size is set to MaxAllocSize - 4MB).
 * The 4MB is saved aside for memory allocation overhead such
 * as allocation set headers.
 */
#define MAX_STRING_BYTES	((Size) (MaxAllocSize - 0x400000))

static int	text_position_ptr_len(char* p1, int len1, char *p2, int len2); 
static void text_position_setup_ptr_len(char* p1, int len1, char* p2, int len2, TextPositionState *state);

static int	text_position_next(int start_pos, TextPositionState *state);
static void text_position_cleanup(TextPositionState *state);
static text *text_substring(Datum str,
			   int32 start,
			   int32 length,
			   bool length_not_specified);
static void appendStringInfoText(StringInfo str, const text *t);


/*****************************************************************************
 *	 CONVERSION ROUTINES EXPORTED FOR USE BY C CODE							 *
 *****************************************************************************/

/*
 * cstring_to_text
 *
 * Create a text value from a null-terminated C string.
 *
 * The new text value is freshly palloc'd with a full-size VARHDR.
 */
text *
cstring_to_text(const char *s)
{
	return cstring_to_text_with_len(s, strlen(s));
}

/*
 * cstring_to_text_with_len
 *
 * Same as cstring_to_text except the caller specifies the string length;
 * the string need not be null_terminated.
 */
text *
cstring_to_text_with_len(const char *s, int len)
{
	text	   *result = (text *) palloc(len + VARHDRSZ);

	SET_VARSIZE(result, len + VARHDRSZ);
	memcpy(VARDATA(result), s, len);

	return result;
}

/*
 * text_to_cstring
 *
 * Create a palloc'd, null-terminated C string from a text value.
 *
 * We support being passed a compressed or toasted text value.
 * This is a bit bogus since such values shouldn't really be referred to as
 * "text *", but it seems useful for robustness.  If we didn't handle that
 * case here, we'd need another routine that did, anyway.
 */
char *
text_to_cstring(const text *t)
{
	/* must cast away the const, unfortunately */
	text	   *tunpacked = pg_detoast_datum_packed((struct varlena *) t);
	int			len = VARSIZE_ANY_EXHDR(tunpacked);
	char	   *result;

	result = (char *) palloc(len + 1);
	memcpy(result, VARDATA_ANY(tunpacked), len);
	result[len] = '\0';

	if (tunpacked != t)
		pfree(tunpacked);
	
	return result;
}

/*
 * text_to_cstring_buffer
 *
 * Copy a text value into a caller-supplied buffer of size dst_len.
 *
 * The text string is truncated if necessary to fit.  The result is
 * guaranteed null-terminated (unless dst_len == 0).
 *
 * We support being passed a compressed or toasted text value.
 * This is a bit bogus since such values shouldn't really be referred to as
 * "text *", but it seems useful for robustness.  If we didn't handle that
 * case here, we'd need another routine that did, anyway.
 */
void
text_to_cstring_buffer(const text *src, char *dst, size_t dst_len)
{
	/* must cast away the const, unfortunately */
	text	   *srcunpacked = pg_detoast_datum_packed((struct varlena *) src);
	size_t		src_len = VARSIZE_ANY_EXHDR(srcunpacked);

	if (dst_len > 0)
	{
		dst_len--;
		if (dst_len >= src_len)
			dst_len = src_len;
		else	/* ensure truncation is encoding-safe */
			dst_len = pg_mbcliplen(VARDATA_ANY(srcunpacked), src_len, dst_len);
		memcpy(dst, VARDATA_ANY(srcunpacked), dst_len);
		dst[dst_len] = '\0';
	}

	if (srcunpacked != src)
		pfree(srcunpacked);
}


/*****************************************************************************
 *	 USER I/O ROUTINES														 *
 *****************************************************************************/


#define VAL(CH)			((CH) - '0')
#define DIG(VAL)		((VAL) + '0')

/*
 *		byteain			- converts from printable representation of byte array
 *
 *		Non-printable characters must be passed as '\nnn' (octal) and are
 *		converted to internal form.  '\' must be passed as '\\'.
 *		ereport(ERROR, ...) if bad form.
 *
 *		BUGS:
 *				The input is scanned twice.
 *				The error checking of input is minimal.
 */
Datum
byteain(PG_FUNCTION_ARGS)
{
	char	   *inputText = PG_GETARG_CSTRING(0);
	char	   *tp;
	char	   *rp;
	int			byte;
	bytea	   *result;

	for (byte = 0, tp = inputText; *tp != '\0'; byte++)
	{
		if (tp[0] != '\\')
			tp++;
		else if ((tp[0] == '\\') &&
				 (tp[1] >= '0' && tp[1] <= '3') &&
				 (tp[2] >= '0' && tp[2] <= '7') &&
				 (tp[3] >= '0' && tp[3] <= '7'))
			tp += 4;
		else if ((tp[0] == '\\') &&
				 (tp[1] == '\\'))
			tp += 2;
		else
		{
			/*
			 * one backslash, not followed by 0 or ### valid octal
			 */
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type bytea"),
							 errOmitLocation(true)));
		}
	}

	byte += VARHDRSZ;
	result = (bytea *) palloc(byte);
	SET_VARSIZE(result, byte);

	tp = inputText;
	rp = VARDATA(result);
	while (*tp != '\0')
	{
		if (tp[0] != '\\')
			*rp++ = *tp++;
		else if ((tp[0] == '\\') &&
				 (tp[1] >= '0' && tp[1] <= '3') &&
				 (tp[2] >= '0' && tp[2] <= '7') &&
				 (tp[3] >= '0' && tp[3] <= '7'))
		{
			byte = VAL(tp[1]);
			byte <<= 3;
			byte += VAL(tp[2]);
			byte <<= 3;
			*rp++ = byte + VAL(tp[3]);
			tp += 4;
		}
		else if ((tp[0] == '\\') &&
				 (tp[1] == '\\'))
		{
			*rp++ = '\\';
			tp += 2;
		}
		else
		{
			/*
			 * We should never get here. The first pass should not allow it.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type bytea"),
							 errOmitLocation(true)));
		}
	}

	PG_RETURN_BYTEA_P(result);
}

/*
 *		byteaout		- converts to printable representation of byte array
 *
 *		Non-printable characters are inserted as '\nnn' (octal) and '\' as
 *		'\\'.
 *
 *		NULL vlena should be an error--returning string with NULL for now.
 */
Datum
byteaout(PG_FUNCTION_ARGS)
{
	bytea	   *vlena = PG_GETARG_BYTEA_P(0);
	char	   *result;
	char	   *vp;
	char	   *rp;
	int			val;			/* holds unprintable chars */
	int			i;
	int			len;

	len = 1;					/* empty string has 1 char */
	vp = VARDATA_ANY(vlena);
	for (i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++)
	{
		if (*vp == '\\')
			len += 2;
		else if ((unsigned char) *vp < 0x20 || (unsigned char) *vp > 0x7e)
			len += 4;
		else
			len++;
	}
	rp = result = (char *) palloc(len);
	vp = VARDATA_ANY(vlena);
	for (i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++)
	{
		if (*vp == '\\')
		{
			*rp++ = '\\';
			*rp++ = '\\';
		}
		else if ((unsigned char) *vp < 0x20 || (unsigned char) *vp > 0x7e)
		{
			val = *vp;
			rp[0] = '\\';
			rp[3] = DIG(val & 07);
			val >>= 3;
			rp[2] = DIG(val & 07);
			val >>= 3;
			rp[1] = DIG(val & 03);
			rp += 4;
		}
		else
			*rp++ = *vp;
	}
	*rp = '\0';
	PG_RETURN_CSTRING(result);
}

/*
 *		bytearecv			- converts external binary format to bytea
 */
Datum
bytearecv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	bytea	   *result;
	int			nbytes;

	nbytes = buf->len - buf->cursor;
	result = (bytea *) palloc(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);
	pq_copymsgbytes(buf, VARDATA(result), nbytes);
	PG_RETURN_BYTEA_P(result);
}

/*
 *		byteasend			- converts bytea to binary format
 *
 * This is a special case: just copy the input...
 */
Datum
byteasend(PG_FUNCTION_ARGS)
{
	bytea	   *vlena = PG_GETARG_BYTEA_P_COPY(0);

	PG_RETURN_BYTEA_P(vlena);
}


/*
 *		textin			- converts "..." to internal representation
 */
Datum
textin(PG_FUNCTION_ARGS)
{
	char	   *inputText = PG_GETARG_CSTRING(0);
	text	   *result;
	int			len;

	len = strlen(inputText);
	result = (text *) palloc(len + VARHDRSZ);
	SET_VARSIZE(result, len + VARHDRSZ);

	memcpy(VARDATA(result), inputText, len);

	PG_RETURN_TEXT_P(result);
}

/*
 *		textout			- converts internal representation to "..."
 */
Datum
textout(PG_FUNCTION_ARGS)
{
	char *result;

	Datum d = PG_GETARG_DATUM(0);
	char *p; void *tofree; int len;
	varattrib_untoast_ptr_len(d, &p, &len, &tofree);

	result = (char *) palloc(len + 1);
	memcpy(result, p, len);
	result[len] = '\0';

	if(tofree != NULL)
		pfree(tofree);

	PG_RETURN_CSTRING(result);
}

/*
 *		textrecv			- converts external binary format to text
 */
Datum
textrecv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	text	   *result;
	char	   *str;
	int			nbytes;

	str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);

	result = (text *) palloc(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);
	memcpy(VARDATA(result), str, nbytes);
	pfree(str);
	PG_RETURN_TEXT_P(result);
}

/*
 *		textsend			- converts text to binary format
 */
Datum
textsend(PG_FUNCTION_ARGS)
{
	text	   *t = PG_GETARG_TEXT_PP(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendtext(&buf, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


/*
 *		unknownin			- converts "..." to internal representation
 */
Datum
unknownin(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);

	/* representation is same as cstring */
	PG_RETURN_CSTRING(pstrdup(str));
}

/*
 *		unknownout			- converts internal representation to "..."
 */
Datum
unknownout(PG_FUNCTION_ARGS)
{
	/* representation is same as cstring */
	char	   *str = PG_GETARG_CSTRING(0);

	PG_RETURN_CSTRING(pstrdup(str));
}

/*
 *		unknownrecv			- converts external binary format to unknown
 */
Datum
unknownrecv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	char	   *str;
	int			nbytes;

	str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
	/* representation is same as cstring */
	PG_RETURN_CSTRING(str);
}

/*
 *		unknownsend			- converts unknown to binary format
 */
Datum
unknownsend(PG_FUNCTION_ARGS)
{
	/* representation is same as cstring */
	char	   *str = PG_GETARG_CSTRING(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendtext(&buf, str, strlen(str));
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


/* ========== PUBLIC ROUTINES ========== */

/*
 * text_length -
 *	Does the real work for textlen()
 *
 *	This is broken out so it can be called directly by other string processing
 *	functions.	Note that the argument is passed as a Datum, to indicate that
 *	it may still be in compressed form.  We can avoid decompressing it at all
 *	in some cases.
 */
static inline int32
text_length(Datum str)
{
	/* fastpath when max encoding length is one */
	if (pg_database_encoding_max_length() == 1)
		PG_RETURN_INT32(toast_raw_datum_size(str) - VARHDRSZ);
	else
	{
		int32 ret;
		char *p; void *tofree; int len;
		varattrib_untoast_ptr_len(str, &p, &len, &tofree);

		ret = pg_mbstrlen_with_len(p, len); 
		if(tofree)
			pfree(tofree);

		PG_RETURN_INT32(ret);
	}
}

/*
 * textlen -
 *	  returns the logical length of a text*
 *	   (which is less than the VARSIZE of the text*)
 */
Datum
textlen(PG_FUNCTION_ARGS)
{
	Datum		str = PG_GETARG_DATUM(0);

	/* try to avoid decompressing argument */
	PG_RETURN_INT32(text_length(str));
}

/*
 * textoctetlen -
 *	  returns the physical length of a text*
 *	   (which is less than the VARSIZE of the text*)
 */
Datum
textoctetlen(PG_FUNCTION_ARGS)
{
	Datum		str = PG_GETARG_DATUM(0);

	/* We need not detoast the input at all */
	PG_RETURN_INT32(toast_raw_datum_size(str) - VARHDRSZ);
}

/* 
 * Generaic concatenation of two varlena.  The varlena may comes in differnt
 * flavors, but they are really the same.  Duplicate the body of the function makes
 * no sense.
 */
static inline Datum generic_varlena_cat(PG_FUNCTION_ARGS)
{
	Datum d0 = PG_GETARG_DATUM(0);
	char *p0; void *tofree0; int len0;

	Datum d1 = PG_GETARG_DATUM(1);
	char *p1; void *tofree1; int len1;

	int len; 
	text *result;
	char *ptr;

	varattrib_untoast_ptr_len(d0, &p0, &len0, &tofree0);
	varattrib_untoast_ptr_len(d1, &p1, &len1, &tofree1);

	len = len0 + len1 + VARHDRSZ;
	result = (text *) palloc(len);

	/* Set size of result string... */
	SET_VARSIZE(result, len);

	/* Fill data field of result string... */
	ptr = VARDATA(result);

	if (len0 > 0)
		memcpy(ptr, p0, len0); 
	if (len1 > 0)
		memcpy(ptr + len0, p1, len1); 

	PG_RETURN_TEXT_P(result);
}

/*
 * textcat -
 *	  takes two text* and returns a text* that is the concatenation of
 *	  the two.
 *
 * Rewritten by Sapa, sapa@hq.icb.chel.su. 8-Jul-96.
 * Updated by Thomas, Thomas.Lockhart@jpl.nasa.gov 1997-07-10.
 * Allocate space for output in all cases.
 * XXX - thomas 1997-07-10
 */
Datum
textcat(PG_FUNCTION_ARGS)
{
	return generic_varlena_cat(fcinfo);
}

/*
 * charlen_to_bytelen()
 *	Compute the number of bytes occupied by n characters starting at *p
 *
 * It is caller's responsibility that there actually are n characters;
 * the string need not be null-terminated.
 */
static int
charlen_to_bytelen(const char *p, int n)
{
	if (pg_database_encoding_max_length() == 1)
	{
		/* Optimization for single-byte encodings */
		return n;
	}
	else
	{
		const char *s;

		for (s = p; n > 0; n--)
			s += pg_mblen(s);

		return s - p;
	}
}

/* find_memory_limited_substring()
 *	Computes the sub-string length in number of characters and number
 *	of bytes where the sub-string consumes up to "memoryLimit" amount of memory.
 *
 *	Parameters:
 *		strStart: starting pointer in the string
 * 		byteLen: number of bytes in the string, starting from strStart
 * 		memoryLimit: max string size in terms of bytes
 *
 * 	Out parameters:
 *		subStringByteLen: length of chosen sub-string in bytes
 *		subStringCharLen: length of chosen sub-string in character count
 *
 * It is caller's responsibility that there actually are byteLen bytes
 * starting from strStart; the string needs not be null-terminated.
 */
static void
find_memory_limited_substring(const char *strStart, int byteLen, int memoryLimit, int *subStringByteLen, int *subStringCharLen)
{
	AssertArg(byteLen > memoryLimit);
	AssertArg(NULL != strStart);
	AssertArg(NULL != subStringCharLen);

	if (pg_database_encoding_max_length() == 1)
	{
		/* Optimization for single-byte encodings */
		*subStringByteLen = byteLen < memoryLimit ? byteLen : memoryLimit;
		*subStringCharLen = *subStringByteLen;

		return;
	}
	else
	{
		const char *strCurPointer = strStart;;

		int consumedBytes = 0;
		int consumedChars = 0;

		while (consumedBytes <= byteLen)
		{
			int curCharBytes = pg_mblen(strCurPointer);
			strCurPointer += curCharBytes;
			consumedChars++;
			consumedBytes += curCharBytes;

			if (consumedBytes > memoryLimit)
			{
				*subStringByteLen = consumedBytes - curCharBytes;
				*subStringCharLen = consumedChars - 1;

				Insist((*subStringByteLen > 0) && (*subStringCharLen > 0));

				return;
			}
		}
	}
}


/*
 * text_substr()
 * Return a substring starting at the specified position.
 * - thomas 1997-12-31
 *
 * Input:
 *	- string
 *	- starting position (is one-based)
 *	- string length
 *
 * If the starting position is zero or less, then return from the start of the string
 *	adjusting the length to be consistent with the "negative start" per SQL92.
 * If the length is less than zero, return the remaining string.
 *
 * Added multibyte support.
 * - Tatsuo Ishii 1998-4-21
 * Changed behavior if starting position is less than one to conform to SQL92 behavior.
 * Formerly returned the entire string; now returns a portion.
 * - Thomas Lockhart 1998-12-10
 * Now uses faster TOAST-slicing interface
 * - John Gray 2002-02-22
 * Remove "#ifdef MULTIBYTE" and test for encoding_max_length instead. Change
 * behaviors conflicting with SQL92 to meet SQL92 (if E = S + L < S throw
 * error; if E < 1, return '', not entire string). Fixed MB related bug when
 * S > LC and < LC + 4 sometimes garbage characters are returned.
 * - Joe Conway 2002-08-10
 */
Datum
text_substr(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(text_substring(PG_GETARG_DATUM(0),
									PG_GETARG_INT32(1),
									PG_GETARG_INT32(2),
									false));
}

/*
 * text_substr_no_len -
 *	  Wrapper to avoid opr_sanity failure due to
 *	  one function accepting a different number of args.
 */
Datum
text_substr_no_len(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(text_substring(PG_GETARG_DATUM(0),
									PG_GETARG_INT32(1),
									-1, true));
}

/*
 * text_substring -
 *	Does the real work for text_substr() and text_substr_no_len()
 *
 *	This is broken out so it can be called directly by other string processing
 *	functions.	Note that the argument is passed as a Datum, to indicate that
 *	it may still be in compressed/toasted form.  We can avoid detoasting all
 *	of it in some cases.
 *
 *	The result is always a freshly palloc'd datum.
 */
static text *
text_substring(Datum str, int32 start, int32 length, bool length_not_specified)
{
	int32		eml = pg_database_encoding_max_length();
	int32		S = start;		/* start position */
	int32		S1;				/* adjusted start position */
	int32		L1;				/* adjusted substring length */

	/* life is easy if the encoding max length is 1 */
	if (eml == 1)
	{
		S1 = Max(S, 1);

		if (length_not_specified)		/* special case - get length to end of
										 * string */
			L1 = -1;
		else
		{
			/* end position */
			int			E = S + length;

			/*
			 * A negative value for L is the only way for the end position to
			 * be before the start. SQL99 says to throw an error.
			 */
			if (E < S)
				ereport(ERROR,
						(errcode(ERRCODE_SUBSTRING_ERROR),
						 errmsg("negative substring length not allowed"),
								 errOmitLocation(true)));

			/*
			 * A zero or negative value for the end position can happen if the
			 * start was negative or one. SQL99 says to return a zero-length
			 * string.
			 */
			if (E < 1)
				return cstring_to_text("");

			L1 = E - S1;
		}

		/*
		 * If the start position is past the end of the string, SQL99 says to
		 * return a zero-length string -- PG_GETARG_TEXT_P_SLICE() will do
		 * that for us. Convert to zero-based starting position
		 */
		return DatumGetTextPSlice(str, S1 - 1, L1);
	}
	else if (eml > 1)
	{
		/*
		 * When encoding max length is > 1, we can't get LC without
		 * detoasting, so we'll grab a conservatively large slice now and go
		 * back later to do the right thing
		 */
		int32		slice_start;
		int32		slice_size;
		int32		slice_strlen;
		text	   *slice;
		int32		E1;
		int32		i;
		char	   *p;
		char	   *s;
		text	   *ret;

		/*
		 * if S is past the end of the string, the tuple toaster will return a
		 * zero-length string to us
		 */
		S1 = Max(S, 1);

		/*
		 * We need to start at position zero because there is no way to know
		 * in advance which byte offset corresponds to the supplied start
		 * position.
		 */
		slice_start = 0;

		if (length_not_specified)		/* special case - get length to end of
										 * string */
			slice_size = L1 = -1;
		else
		{
			int			E = S + length;

			/*
			 * A negative value for L is the only way for the end position to
			 * be before the start. SQL99 says to throw an error.
			 */
			if (E < S)
				ereport(ERROR,
						(errcode(ERRCODE_SUBSTRING_ERROR),
						 errmsg("negative substring length not allowed"),
								 errOmitLocation(true)));

			/*
			 * A zero or negative value for the end position can happen if the
			 * start was negative or one. SQL99 says to return a zero-length
			 * string.
			 */
			if (E < 1)
				return cstring_to_text("");

			/*
			 * if E is past the end of the string, the tuple toaster will
			 * truncate the length for us
			 */
			L1 = E - S1;

			/*
			 * Total slice size in bytes can't be any longer than the start
			 * position plus substring length times the encoding max length.
			 */
			slice_size = (S1 + L1) * eml;
		}

		/*
		 * If we're working with an untoasted source, no need to do an extra
		 * copying step.
		 */
		if (VARATT_IS_COMPRESSED(DatumGetPointer(str)) ||
			VARATT_IS_EXTERNAL(DatumGetPointer(str)))
			slice = DatumGetTextPSlice(str, slice_start, slice_size);
		else
			slice = (text *) DatumGetPointer(str);

		/* see if we got back an empty string */
		if (VARSIZE_ANY_EXHDR(slice) == 0)
		{
			if (slice != (text *) DatumGetPointer(str))
				pfree(slice);
			return cstring_to_text("");
		}

		/* Now we can get the actual length of the slice in MB characters */
		slice_strlen = pg_mbstrlen_with_len(VARDATA_ANY(slice),
											VARSIZE_ANY_EXHDR(slice));

		/*
		 * Check that the start position wasn't > slice_strlen. If so, SQL99
		 * says to return a zero-length string.
		 */
		if (S1 > slice_strlen)
		{
			if (slice != (text *) DatumGetPointer(str))
				pfree(slice);
			return cstring_to_text("");
		}

		/*
		 * Adjust L1 and E1 now that we know the slice string length. Again
		 * remember that S1 is one based, and slice_start is zero based.
		 */
		if (L1 > -1)
			E1 = Min(S1 + L1, slice_start + 1 + slice_strlen);
		else
			E1 = slice_start + 1 + slice_strlen;

		/*
		 * Find the start position in the slice; remember S1 is not zero based
		 */
		p = VARDATA_ANY(slice);
		for (i = 0; i < S1 - 1; i++)
			p += pg_mblen(p);

		/* hang onto a pointer to our start position */
		s = p;

		/*
		 * Count the actual bytes used by the substring of the requested
		 * length.
		 */
		for (i = S1; i < E1; i++)
			p += pg_mblen(p);

		ret = (text *) palloc(VARHDRSZ + (p - s));
		SET_VARSIZE(ret, VARHDRSZ + (p - s));
		memcpy(VARDATA(ret), s, (p - s));

		if (slice != (text *) DatumGetPointer(str))
			pfree(slice);

		return ret;
	}
	else
		elog(ERROR, "invalid backend encoding: encoding max length < 1");

	/* not reached: suppress compiler warning */
	return NULL;
}

/*
 * textpos -
 *	  Return the position of the specified substring.
 *	  Implements the SQL92 POSITION() function.
 *	  Ref: A Guide To The SQL Standard, Date & Darwen, 1997
 * - thomas 1997-07-27
 */
Datum
textpos(PG_FUNCTION_ARGS)
{
	Datum d0 = PG_GETARG_DATUM(0);
	char *p0; void *tofree0; int len0;

	Datum d1 = PG_GETARG_DATUM(1);
	char *p1; void *tofree1; int len1;
	
	int32 pos;

	varattrib_untoast_ptr_len(d0, &p0, &len0, &tofree0);
	varattrib_untoast_ptr_len(d1, &p1, &len1, &tofree1);

	pos = text_position_ptr_len(p0, len0, p1, len1);

	if(tofree0)
		pfree(tofree0);
	if(tofree1)
		pfree(tofree1);

	PG_RETURN_INT32(pos);
}

/*
 * text_position -
 *	Does the real work for textpos()
 *
 * Inputs:
 *		p1, len1 - string to be searched
 *		p2, len2 - pattern to match within t1
 * Result:
 *		Character index of the first matched char, starting from 1,
 *		or 0 if no match.
 *
 *	This is broken out so it can be called directly by other string processing
 *	functions.
 */
static int 
text_position_ptr_len(char* p1, int len1, char* p2, int len2)
{
	TextPositionState state = 
		{
		0, /* use_wchar */
		NULL, /* str1 */
		NULL, /* str2 */
		NULL, /* wstr1 */
		NULL, /* wstr2 */
		0, /* len1 */
		0, /* len2 */
		};


	int result;

	text_position_setup_ptr_len(p1, len1, p2, len2, &state);

	result = text_position_next(1, &state);
	text_position_cleanup(&state);
	return result;
}

/*
 * text_position_setup, text_position_next, text_position_cleanup -
 *	Component steps of text_position()
 *
 * These are broken out so that a string can be efficiently searched for
 * multiple occurrences of the same pattern.  text_position_next may be
 * called multiple times with increasing values of start_pos, which is
 * the 1-based character position to start the search from.  The "state"
 * variable is normally just a local variable in the caller.
 */

/* Set up text postion, using pointer and len. */
static void 
text_position_setup_ptr_len(char* p1, int len1, char* p2, int len2, TextPositionState *state)
{
	if (pg_database_encoding_max_length() == 1)
	{
		/* simple case - single byte encoding */
		state->use_wchar = false;
		state->str1 = p1; 
		state->str2 = p2; 
		state->len1 = len1;
		state->len2 = len2;
	}
	else
	{
		/* not as simple - multibyte encoding */
		pg_wchar   *wp1,
				   *wp2;

		wp1 = (pg_wchar *) palloc((len1 + 1) * sizeof(pg_wchar));
		len1 = pg_mb2wchar_with_len(p1, wp1, len1);
		wp2 = (pg_wchar *) palloc((len2 + 1) * sizeof(pg_wchar));
		len2 = pg_mb2wchar_with_len(p2, wp2, len2);

		state->use_wchar = true;
		state->wstr1 = wp1;
		state->wstr2 = wp2;
		state->len1 = len1;
		state->len2 = len2;
	}
}
static int
text_position_next(int start_pos, TextPositionState *state)
{
	int			pos = 0,
				p,
				px;

	Assert(start_pos > 0);		/* else caller error */

	if (state->len2 <= 0)
		return start_pos;		/* result for empty pattern */

	if (!state->use_wchar)
	{
		/* simple case - single byte encoding */
		char	   *p1 = state->str1;
		char	   *p2 = state->str2;

		/* no use in searching str past point where search_str will fit */
		px = (state->len1 - state->len2);

		p1 += start_pos - 1;

		for (p = start_pos - 1; p <= px; p++)
		{
			if ((*p1 == *p2) && (strncmp(p1, p2, state->len2) == 0))
			{
				pos = p + 1;
				break;
			}
			p1++;
		}
	}
	else
	{
		/* not as simple - multibyte encoding */
		pg_wchar   *p1 = state->wstr1;
		pg_wchar   *p2 = state->wstr2;

		/* no use in searching str past point where search_str will fit */
		px = (state->len1 - state->len2);

		p1 += start_pos - 1;

		for (p = start_pos - 1; p <= px; p++)
		{
			if ((*p1 == *p2) && (pg_wchar_strncmp(p1, p2, state->len2) == 0))
			{
				pos = p + 1;
				break;
			}
			p1++;
		}
	}

	return pos;
}

static void
text_position_cleanup(TextPositionState *state)
{
	if (state->use_wchar)
	{
		pfree(state->wstr1);
		pfree(state->wstr2);
	}
}

/* varstr_cmp()
 * Comparison function for text strings with given lengths.
 * Includes locale support, but must copy strings to temporary memory
 *	to allow null-termination for inputs to strcoll().
 * Returns -1, 0 or 1
 */
int
varstr_cmp(char *arg1, int len1, char *arg2, int len2)
{
	int			result;

	/*
	 * Unfortunately, there is no strncoll(), so in the non-C locale case we
	 * have to do some memory copying.	This turns out to be significantly
	 * slower, so we optimize the case where LC_COLLATE is C.  We also try to
	 * optimize relatively-short strings by avoiding palloc/pfree overhead.
	 */
	if (lc_collate_is_c())
	{
		result = strncmp(arg1, arg2, Min(len1, len2));
		if ((result == 0) && (len1 != len2))
			result = (len1 < len2) ? -1 : 1;
	}
	else
	{
#define STACKBUFLEN		1024

		char		a1buf[STACKBUFLEN];
		char		a2buf[STACKBUFLEN];
		char	   *a1p,
				   *a2p;

#ifdef WIN32
		/* Win32 does not have UTF-8, so we need to map to UTF-16 */
		if (GetDatabaseEncoding() == PG_UTF8)
		{
			int			a1len;
			int			a2len;
			int			r;

			if (len1 >= STACKBUFLEN / 2)
			{
				a1len = len1 * 2 + 2;
				a1p = palloc(a1len);
			}
			else
			{
				a1len = STACKBUFLEN;
				a1p = a1buf;
			}
			if (len2 >= STACKBUFLEN / 2)
			{
				a2len = len2 * 2 + 2;
				a2p = palloc(a2len);
			}
			else
			{
				a2len = STACKBUFLEN;
				a2p = a2buf;
			}

			/* stupid Microsloth API does not work for zero-length input */
			if (len1 == 0)
				r = 0;
			else
			{
				r = MultiByteToWideChar(CP_UTF8, 0, arg1, len1,
										(LPWSTR) a1p, a1len / 2);
				if (!r)
					ereport(ERROR,
					 (errmsg("could not convert string to UTF-16: error %lu",
							 GetLastError())));
			}
			((LPWSTR) a1p)[r] = 0;

			if (len2 == 0)
				r = 0;
			else
			{
				r = MultiByteToWideChar(CP_UTF8, 0, arg2, len2,
										(LPWSTR) a2p, a2len / 2);
				if (!r)
					ereport(ERROR,
					 (errmsg("could not convert string to UTF-16: error %lu",
							 GetLastError())));
			}
			((LPWSTR) a2p)[r] = 0;

			errno = 0;
			result = wcscoll((LPWSTR) a1p, (LPWSTR) a2p);
			if (result == 2147483647)	/* _NLSCMPERROR; missing from mingw
										 * headers */
				ereport(ERROR,
						(errmsg("could not compare Unicode strings: %m")));

			/*
			 * In some locales wcscoll() can claim that nonidentical strings
			 * are equal.  Believing that would be bad news for a number of
			 * reasons, so we follow Perl's lead and sort "equal" strings
			 * according to strcmp (on the UTF-8 representation).
			 */
			if (result == 0)
			{
				result = strncmp(arg1, arg2, Min(len1, len2));
				if ((result == 0) && (len1 != len2))
					result = (len1 < len2) ? -1 : 1;
			}

			if (a1p != a1buf)
				pfree(a1p);
			if (a2p != a2buf)
				pfree(a2p);

			return result;
		}
#endif   /* WIN32 */

		if (len1 >= STACKBUFLEN)
			a1p = (char *) palloc(len1 + 1);
		else
			a1p = a1buf;
		if (len2 >= STACKBUFLEN)
			a2p = (char *) palloc(len2 + 1);
		else
			a2p = a2buf;

		memcpy(a1p, arg1, len1);
		a1p[len1] = '\0';
		memcpy(a2p, arg2, len2);
		a2p[len2] = '\0';

		result = gp_strcoll(a1p, a2p);

		/*
		 * In some locales strcoll() can claim that nonidentical strings are
		 * equal.  Believing that would be bad news for a number of reasons,
		 * so we follow Perl's lead and sort "equal" strings according to
		 * strcmp().
		 */
		if (result == 0)
			result = strcmp(a1p, a2p);

		if (a1p != a1buf)
			pfree(a1p);
		if (a2p != a2buf)
			pfree(a2p);
	}

	return result;
}

static inline int
text_cmp_datum(Datum d0, Datum d1)
{
	char *p0; void *tofree0; int len0;
	char *p1; void *tofree1; int len1;
	int result;

	varattrib_untoast_ptr_len(d0, &p0, &len0, &tofree0);
	varattrib_untoast_ptr_len(d1, &p1, &len1, &tofree1);

	result = varstr_cmp(p0, len0, p1, len1);

	if(tofree0)
		pfree(tofree0);
	if(tofree1)
		pfree(tofree1);

	return result;
}

static inline Datum generic_varlena_eq(PG_FUNCTION_ARGS)
{
	bool		result;

	Datum d0 = PG_GETARG_DATUM(0);
	char *p0; void *tofree0; int len0;

	Datum d1 = PG_GETARG_DATUM(1);
	char *p1; void *tofree1; int len1;

	varattrib_untoast_ptr_len(d0, &p0, &len0, &tofree0);
	varattrib_untoast_ptr_len(d1, &p1, &len1, &tofree1);

	/*
	 * Since we only care about equality or not-equality, we can avoid all the
	 * expense of strcoll() here, and just do bitwise comparison.
	 */
	if(len0 != len1)
		result = false;
	else
		result = (memcmp(p0, p1, len1) == 0);

	if(tofree0)
		pfree(tofree0);
	if(tofree1)
		pfree(tofree1);
	
	PG_RETURN_BOOL(result);
}

/*
 * Comparison functions for text strings.
 *
 * Note: btree indexes need these routines not to leak memory; therefore,
 * be careful to free working copies of toasted datums.  Most places don't
 * need to be so careful.
 */

Datum
texteq(PG_FUNCTION_ARGS)
{
	return generic_varlena_eq(fcinfo);
}

Datum
textne(PG_FUNCTION_ARGS)
{
	Datum d = generic_varlena_eq(fcinfo);
	return (d==0 ? 1 : 0);
}

Datum
text_lt(PG_FUNCTION_ARGS)
{
	Datum d0 = PG_GETARG_DATUM(0);
	Datum d1 = PG_GETARG_DATUM(1);

	bool result = (text_cmp_datum(d0, d1) < 0); 
	PG_RETURN_BOOL(result);
}

Datum
text_le(PG_FUNCTION_ARGS)
{	
	Datum d0 = PG_GETARG_DATUM(0);
	Datum d1 = PG_GETARG_DATUM(1);

	bool result = (text_cmp_datum(d0, d1) <= 0); 
	PG_RETURN_BOOL(result);
}

Datum
text_gt(PG_FUNCTION_ARGS)
{
	Datum d0 = PG_GETARG_DATUM(0);
	Datum d1 = PG_GETARG_DATUM(1);

	bool result = (text_cmp_datum(d0, d1) > 0); 
	PG_RETURN_BOOL(result);
}

Datum
text_ge(PG_FUNCTION_ARGS)
{
	Datum d0 = PG_GETARG_DATUM(0);
	Datum d1 = PG_GETARG_DATUM(1);

	bool result = (text_cmp_datum(d0, d1) >= 0); 
	PG_RETURN_BOOL(result);
}

Datum
bttextcmp(PG_FUNCTION_ARGS)
{
	Datum d0 = PG_GETARG_DATUM(0);
	Datum d1 = PG_GETARG_DATUM(1);

	int result = text_cmp_datum(d0, d1);
	PG_RETURN_INT32(result);
}


Datum
text_larger(PG_FUNCTION_ARGS)
{
	Datum d0 = PG_GETARG_DATUM(0);
	Datum d1 = PG_GETARG_DATUM(1);

	int cmp = text_cmp_datum(d0, d1);
	return (cmp > 0 ? d0 : d1);
}

Datum
text_smaller(PG_FUNCTION_ARGS)
{
	Datum d0 = PG_GETARG_DATUM(0);
	Datum d1 = PG_GETARG_DATUM(1);

	int cmp = text_cmp_datum(d0, d1);
	return (cmp < 0 ? d0 : d1);
}


/*
 * The following operators support character-by-character comparison
 * of text data types, to allow building indexes suitable for LIKE
 * clauses.
 */
static inline int generic_varlena_cmp(PG_FUNCTION_ARGS)
{
	Datum d0 = PG_GETARG_DATUM(0);
	Datum d1 = PG_GETARG_DATUM(1);

	int			result;

	char *p0; void *tofree0; int len0;
	char *p1; void *tofree1; int len1;

	varattrib_untoast_ptr_len(d0, &p0, &len0, &tofree0);
	varattrib_untoast_ptr_len(d1, &p1, &len1, &tofree1);

	result = memcmp(p0, p1, Min(len0, len1));

	if(tofree0)
		pfree(tofree0);
	if(tofree1)
		pfree(tofree1);

	if(result != 0)
		return result;

	if (len0 < len1)
		return -1;
	else if (len0 > len1)
		return 1;
	else
		return 0;
}

Datum
text_pattern_lt(PG_FUNCTION_ARGS)
{
	int result = generic_varlena_cmp(fcinfo); 
	PG_RETURN_BOOL(result < 0);
}


Datum
text_pattern_le(PG_FUNCTION_ARGS)
{
	int result = generic_varlena_cmp(fcinfo); 
	PG_RETURN_BOOL(result <= 0);
}



Datum
text_pattern_ge(PG_FUNCTION_ARGS)
{
	int result = generic_varlena_cmp(fcinfo); 
	PG_RETURN_BOOL(result >= 0);
}


Datum
text_pattern_gt(PG_FUNCTION_ARGS)
{
	int result = generic_varlena_cmp(fcinfo); 
	PG_RETURN_BOOL(result > 0);
}



Datum
bttext_pattern_cmp(PG_FUNCTION_ARGS)
{
	int result = generic_varlena_cmp(fcinfo); 
	PG_RETURN_INT32(result); 
}

/*-------------------------------------------------------------
 * byteaoctetlen
 *
 * get the number of bytes contained in an instance of type 'bytea'
 *-------------------------------------------------------------
 */
Datum
byteaoctetlen(PG_FUNCTION_ARGS)
{
	Datum		str = PG_GETARG_DATUM(0);

	/* We need not detoast the input at all */
	PG_RETURN_INT32(toast_raw_datum_size(str) - VARHDRSZ);
}

/*
 * byteacat -
 *	  takes two bytea* and returns a bytea* that is the concatenation of
 *	  the two.
 *
 * Cloned from textcat and modified as required.
 */
Datum
byteacat(PG_FUNCTION_ARGS)
{
	return generic_varlena_cat(fcinfo);
}

#define PG_STR_GET_BYTEA(str_) \
	DatumGetByteaP(DirectFunctionCall1(byteain, CStringGetDatum(str_)))

/*
 * bytea_substr()
 * Return a substring starting at the specified position.
 * Cloned from text_substr and modified as required.
 *
 * Input:
 *	- string
 *	- starting position (is one-based)
 *	- string length (optional)
 *
 * If the starting position is zero or less, then return from the start of the string
 * adjusting the length to be consistent with the "negative start" per SQL92.
 * If the length is less than zero, an ERROR is thrown. If no third argument
 * (length) is provided, the length to the end of the string is assumed.
 */
Datum
bytea_substr(PG_FUNCTION_ARGS)
{
	int			S = PG_GETARG_INT32(1); /* start position */
	int			S1;				/* adjusted start position */
	int			L1;				/* adjusted substring length */

	S1 = Max(S, 1);

	if (fcinfo->nargs == 2)
	{
		/*
		 * Not passed a length - PG_GETARG_BYTEA_P_SLICE() grabs everything to
		 * the end of the string if we pass it a negative value for length.
		 */
		L1 = -1;
	}
	else
	{
		/* end position */
		int			E = S + PG_GETARG_INT32(2);

		/*
		 * A negative value for L is the only way for the end position to be
		 * before the start. SQL99 says to throw an error.
		 */
		if (E < S)
			ereport(ERROR,
					(errcode(ERRCODE_SUBSTRING_ERROR),
					 errmsg("negative substring length not allowed"),
							 errOmitLocation(true)));

		/*
		 * A zero or negative value for the end position can happen if the
		 * start was negative or one. SQL99 says to return a zero-length
		 * string.
		 */
		if (E < 1)
			PG_RETURN_BYTEA_P(PG_STR_GET_BYTEA(""));

		L1 = E - S1;
	}

	/*
	 * If the start position is past the end of the string, SQL99 says to
	 * return a zero-length string -- PG_GETARG_TEXT_P_SLICE() will do that
	 * for us. Convert to zero-based starting position
	 */
	PG_RETURN_BYTEA_P(PG_GETARG_BYTEA_P_SLICE(0, S1 - 1, L1));
}

/*
 * bytea_substr_no_len -
 *	  Wrapper to avoid opr_sanity failure due to
 *	  one function accepting a different number of args.
 */
Datum
bytea_substr_no_len(PG_FUNCTION_ARGS)
{
	return bytea_substr(fcinfo);
}

/*
 * byteapos -
 *	  Return the position of the specified substring.
 *	  Implements the SQL92 POSITION() function.
 * Cloned from textpos and modified as required.
 */
Datum
byteapos(PG_FUNCTION_ARGS)
{
	Datum d0 = PG_GETARG_DATUM(0);
	char *p0; void *tofree0; int len0;

	Datum d1 = PG_GETARG_DATUM(1);
	char *p1; void *tofree1; int len1;

	int			pos;
	int			px, p;

	varattrib_untoast_ptr_len(d0, &p0, &len0, &tofree0);
	varattrib_untoast_ptr_len(d1, &p1, &len1, &tofree1);

	/* empty pattern */
	if(len1 == 0)
		PG_RETURN_INT32(1);

	pos = 0;
	px = (len0 - len1);
	for (p = 0; p <= px; p++)
	{
		if ((*p1 == *p0) && (memcmp(p0, p1, len1) == 0))
		{
			pos = p + 1;
			break;
		};
		p0++;
	};

	if(tofree0)
		pfree(tofree0);
	if(tofree1)
		pfree(tofree1);

	PG_RETURN_INT32(pos);
}

/*-------------------------------------------------------------
 * byteaGetByte
 *
 * this routine treats "bytea" as an array of bytes.
 * It returns the Nth byte (a number between 0 and 255).
 *-------------------------------------------------------------
 */
Datum
byteaGetByte(PG_FUNCTION_ARGS)
{
	Datum d0 = PG_GETARG_DATUM(0);
	char *p0; void *tofree0; int len0;

	int32		n = PG_GETARG_INT32(1);
	int32 result;

	varattrib_untoast_ptr_len(d0, &p0, &len0, &tofree0);

	if (n < 0 || n >= len0)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("index %d out of valid range, 0..%d",
						n, len0 - 1),
								 errOmitLocation(true)));

	result = (unsigned char) p0[n];

	if(tofree0)
		pfree(tofree0);

	PG_RETURN_INT32(result);
}

/*-------------------------------------------------------------
 * byteaGetBit
 *
 * This routine treats a "bytea" type like an array of bits.
 * It returns the value of the Nth bit (0 or 1).
 *
 *-------------------------------------------------------------
 */
Datum
byteaGetBit(PG_FUNCTION_ARGS)
{
	Datum d0 = PG_GETARG_DATUM(0);
	char *p0; void *tofree0; int len0;

	int32		n = PG_GETARG_INT32(1);

	int			byteNo, bitNo;
	int			result;

	varattrib_untoast_ptr_len(d0, &p0, &len0, &tofree0);

	if (n < 0 || n >= len0 * 8)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("index %d out of valid range, 0..%d",
						n, len0 * 8 - 1),
								 errOmitLocation(true)));

	byteNo = n / 8;
	bitNo = n % 8;

	result = (unsigned char) p0[byteNo]; 

	if(tofree0)
		pfree(tofree0);

	if (result & (1 << bitNo))
		PG_RETURN_INT32(1);
	else
		PG_RETURN_INT32(0);
}

/*-------------------------------------------------------------
 * byteaSetByte
 *
 * Given an instance of type 'bytea' creates a new one with
 * the Nth byte set to the given value.
 *
 *-------------------------------------------------------------
 */
Datum
byteaSetByte(PG_FUNCTION_ARGS)
{
	bytea	   *v = PG_GETARG_BYTEA_P(0);
	int32		n = PG_GETARG_INT32(1);
	int32		newByte = PG_GETARG_INT32(2);
	int			len;
	bytea	   *res;

	len = VARSIZE(v) - VARHDRSZ;

	if (n < 0 || n >= len)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("index %d out of valid range, 0..%d",
						n, len - 1),
								 errOmitLocation(true)));

	/*
	 * Make a copy of the original varlena.
	 */
	res = (bytea *) palloc(VARSIZE(v));
	memcpy((char *) res, (char *) v, VARSIZE(v));

	/*
	 * Now set the byte.
	 */
	((unsigned char *) VARDATA(res))[n] = newByte;

	PG_RETURN_BYTEA_P(res);
}

/*-------------------------------------------------------------
 * byteaSetBit
 *
 * Given an instance of type 'bytea' creates a new one with
 * the Nth bit set to the given value.
 *
 *-------------------------------------------------------------
 */
Datum
byteaSetBit(PG_FUNCTION_ARGS)
{
	bytea	   *v = PG_GETARG_BYTEA_P(0);
	int32		n = PG_GETARG_INT32(1);
	int32		newBit = PG_GETARG_INT32(2);
	bytea	   *res;
	int			len;
	int			oldByte,
				newByte;
	int			byteNo,
				bitNo;

	len = VARSIZE(v) - VARHDRSZ;

	if (n < 0 || n >= len * 8)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("index %d out of valid range, 0..%d",
						n, len * 8 - 1),
								 errOmitLocation(true)));

	byteNo = n / 8;
	bitNo = n % 8;

	/*
	 * sanity check!
	 */
	if (newBit != 0 && newBit != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("new bit must be 0 or 1"),
						 errOmitLocation(true)));

	/*
	 * Make a copy of the original varlena.
	 */
	res = (bytea *) palloc(VARSIZE(v));
	memcpy((char *) res, (char *) v, VARSIZE(v));

	/*
	 * Update the byte.
	 */
	oldByte = ((unsigned char *) VARDATA(res))[byteNo];

	if (newBit == 0)
		newByte = oldByte & (~(1 << bitNo));
	else
		newByte = oldByte | (1 << bitNo);

	((unsigned char *) VARDATA(res))[byteNo] = newByte;

	PG_RETURN_BYTEA_P(res);
}


/* text_name()
 * Converts a text type to a Name type.
 */
Datum
text_name(PG_FUNCTION_ARGS)
{
	Datum d0 = PG_GETARG_DATUM(0);
	char *p0; void *tofree0; int len0;
	Name		result;

	varattrib_untoast_ptr_len(d0, &p0, &len0, &tofree0);

	/* Truncate oversize input */
	if (len0 >= NAMEDATALEN)
		len0 = NAMEDATALEN - 1;

	result = (Name) palloc(NAMEDATALEN);
	memcpy(NameStr(*result), p0, len0); 

	/* now null pad to full length... */
	while (len0 < NAMEDATALEN)
	{
		*(NameStr(*result) + len0) = '\0';
		len0++;
	}

	if(tofree0)
		pfree(tofree0);

	PG_RETURN_NAME(result);
}

/* name_text()
 * Converts a Name type to a text type.
 */
Datum
name_text(PG_FUNCTION_ARGS)
{
	Name		s = PG_GETARG_NAME(0);

	PG_RETURN_TEXT_P(cstring_to_text(NameStr(*s)));
}


/*
 * textToQualifiedNameList - convert a text object to list of names
 *
 * This implements the input parsing needed by nextval() and other
 * functions that take a text parameter representing a qualified name.
 * We split the name at dots, downcase if not double-quoted, and
 * truncate names if they're too long.
 */
List *
textToQualifiedNameList(text *textval)
{
	char	   *rawname;
	List	   *result = NIL;
	List	   *namelist;
	ListCell   *l;

	/* Convert to C string (handles possible detoasting). */
	/* Note we rely on being able to modify rawname below. */
	rawname = text_to_cstring(textval);

	if (!SplitIdentifierString(rawname, '.', &namelist))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("invalid name syntax"),
						 errOmitLocation(true)));

	if (namelist == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("invalid name syntax"),
						 errOmitLocation(true)));

	foreach(l, namelist)
	{
		char	   *curname = (char *) lfirst(l);

		result = lappend(result, makeString(pstrdup(curname)));
	}

	pfree(rawname);
	list_free(namelist);

	return result;
}

/*
 * SplitIdentifierString --- parse a string containing identifiers
 *
 * This is the guts of textToQualifiedNameList, and is exported for use in
 * other situations such as parsing GUC variables.	In the GUC case, it's
 * important to avoid memory leaks, so the API is designed to minimize the
 * amount of stuff that needs to be allocated and freed.
 *
 * Inputs:
 *	rawstring: the input string; must be overwritable!	On return, it's
 *			   been modified to contain the separated identifiers.
 *	separator: the separator punctuation expected between identifiers
 *			   (typically '.' or ',').	Whitespace may also appear around
 *			   identifiers.
 * Outputs:
 *	namelist: filled with a palloc'd list of pointers to identifiers within
 *			  rawstring.  Caller should list_free() this even on error return.
 *
 * Returns TRUE if okay, FALSE if there is a syntax error in the string.
 *
 * Note that an empty string is considered okay here, though not in
 * textToQualifiedNameList.
 */
bool
SplitIdentifierString(char *rawstring, char separator,
					  List **namelist)
{
	char	   *nextp = rawstring;
	bool		done = false;

	*namelist = NIL;

	while (isspace((unsigned char) *nextp))
		nextp++;				/* skip leading whitespace */

	if (*nextp == '\0')
		return true;			/* allow empty string */

	/* At the top of the loop, we are at start of a new identifier. */
	do
	{
		char	   *curname;
		char	   *endp;

		if (*nextp == '\"')
		{
			/* Quoted name --- collapse quote-quote pairs, no downcasing */
			curname = nextp + 1;
			for (;;)
			{
				endp = strchr(nextp + 1, '\"');
				if (endp == NULL)
					return false;		/* mismatched quotes */
				if (endp[1] != '\"')
					break;		/* found end of quoted name */
				/* Collapse adjacent quotes into one quote, and look again */
				memmove(endp, endp + 1, strlen(endp));
				nextp = endp;
			}
			/* endp now points at the terminating quote */
			nextp = endp + 1;
		}
		else
		{
			/* Unquoted name --- extends to separator or whitespace */
			char	   *downname;
			int			len;

			curname = nextp;
			while (*nextp && *nextp != separator &&
				   !isspace((unsigned char) *nextp))
				nextp++;
			endp = nextp;
			if (curname == nextp)
				return false;	/* empty unquoted name not allowed */

			/*
			 * Downcase the identifier, using same code as main lexer does.
			 *
			 * XXX because we want to overwrite the input in-place, we cannot
			 * support a downcasing transformation that increases the string
			 * length.	This is not a problem given the current implementation
			 * of downcase_truncate_identifier, but we'll probably have to do
			 * something about this someday.
			 */
			len = endp - curname;
			downname = downcase_truncate_identifier(curname, len, false);
			Assert(strlen(downname) <= len);
			strncpy(curname, downname, len);
			pfree(downname);
		}

		while (isspace((unsigned char) *nextp))
			nextp++;			/* skip trailing whitespace */

		if (*nextp == separator)
		{
			nextp++;
			while (isspace((unsigned char) *nextp))
				nextp++;		/* skip leading whitespace for next */
			/* we expect another name, so done remains false */
		}
		else if (*nextp == '\0')
			done = true;
		else
			return false;		/* invalid syntax */

		/* Now safe to overwrite separator with a null */
		*endp = '\0';

		/* Truncate name if it's overlength */
		truncate_identifier(curname, strlen(curname), false);

		/*
		 * Finished isolating current name --- add it to list
		 */
		*namelist = lappend(*namelist, curname);

		/* Loop back if we didn't reach end of string */
	} while (!done);

	return true;
}


/*****************************************************************************
 *	Comparison Functions used for bytea
 *
 * Note: btree indexes need these routines not to leak memory; therefore,
 * be careful to free working copies of toasted datums.  Most places don't
 * need to be so careful.
 *****************************************************************************/

Datum
byteaeq(PG_FUNCTION_ARGS)
{
	return generic_varlena_eq(fcinfo);
}

Datum
byteane(PG_FUNCTION_ARGS)
{
	Datum d = generic_varlena_eq(fcinfo);
	return (d==0 ? 1 : 0);
}

Datum
bytealt(PG_FUNCTION_ARGS)
{
	int result = generic_varlena_cmp(fcinfo);
	PG_RETURN_BOOL(result < 0); 
}

Datum
byteale(PG_FUNCTION_ARGS)
{
	int result = generic_varlena_cmp(fcinfo);
	PG_RETURN_BOOL(result <= 0); 
}

Datum
byteagt(PG_FUNCTION_ARGS)
{
	int result = generic_varlena_cmp(fcinfo);
	PG_RETURN_BOOL(result > 0); 
}

Datum
byteage(PG_FUNCTION_ARGS)
{
	int result = generic_varlena_cmp(fcinfo);
	PG_RETURN_BOOL(result >= 0); 
}

Datum
byteacmp(PG_FUNCTION_ARGS)
{
	int result = generic_varlena_cmp(fcinfo);
	PG_RETURN_INT32(result); 
}

/*
 * appendStringInfoText
 *
 * Append a text to str.
 * Like appendStringInfoString(str, PG_TEXT_GET_STR(s)) but faster.
 */
static void
appendStringInfoText(StringInfo str, const text *t)
{
	appendBinaryStringInfo(str, VARDATA_ANY((void *) t), VARSIZE_ANY_EXHDR((void *) t));
}

/*
 * replace_text
 * replace all occurrences of 'old_sub_str' in 'orig_str'
 * with 'new_sub_str' to form 'new_str'
 *
 * returns 'orig_str' if 'old_sub_str' == '' or 'orig_str' == ''
 * otherwise returns 'new_str'
 */
Datum
replace_text(PG_FUNCTION_ARGS)
{
	Datum d0 = PG_GETARG_DATUM(0);
	char *p0; void *tofree0; int len0;

	Datum d1 = PG_GETARG_DATUM(1);
	char *p1; void *tofree1; int len1;

	Datum d2 = PG_GETARG_DATUM(2);
	char *p2; void *tofree2; int len2;

	int from_sub_text_len; 

	TextPositionState state = 
		{
		0, /* use_wchar */
		NULL, /* str1 */
		NULL, /* str2 */
		NULL, /* wstr1 */
		NULL, /* wstr2 */
		0, /* len1 */
		0, /* len2 */
		};
	text	   *ret_text;
	int			start_posn;
	int			curr_posn;
	int			chunk_len;
	char	   *start_ptr;
	StringInfoData str;

	varattrib_untoast_ptr_len(d0, &p0, &len0, &tofree0);
	varattrib_untoast_ptr_len(d1, &p1, &len1, &tofree1);
	varattrib_untoast_ptr_len(d2, &p2, &len2, &tofree2);

	if(pg_database_encoding_max_length() == 1)
		from_sub_text_len = len1;
	else
		from_sub_text_len = pg_mbstrlen_with_len(p1, len1);

	if (len0 == 0 || from_sub_text_len == 0)
	{
		if(tofree0)
			pfree(tofree0);
		if(tofree1)
			pfree(tofree1);
		if(tofree2)
			pfree(tofree2);
		return d0;
	}

	text_position_setup_ptr_len(p0, len0, p1, len1, &state); 

	start_posn = 1;
	curr_posn = text_position_next(1, &state);

	/* When the from_sub_text is not found, there is nothing to do. */
	if (curr_posn == 0)
	{
		if(tofree0)
			pfree(tofree0);
		if(tofree1)
			pfree(tofree1);
		if(tofree2)
			pfree(tofree2);

		text_position_cleanup(&state);
		return d0;
	}

	/* start_ptr points to the start_posn'th character of src_text */
	start_ptr = p0; 

	initStringInfo(&str);

	do
	{
		/* copy the data skipped over by last text_position_next() */
		chunk_len = charlen_to_bytelen(start_ptr, curr_posn - start_posn);
		appendBinaryStringInfo(&str, start_ptr, chunk_len);
		appendBinaryStringInfo(&str, p2, len2);

		start_posn = curr_posn;
		start_ptr += chunk_len;
		start_posn += from_sub_text_len;
		start_ptr += charlen_to_bytelen(start_ptr, from_sub_text_len);

		curr_posn = text_position_next(start_posn, &state);
	}
	while (curr_posn > 0);

	/* copy trailing data */
	chunk_len = ((char *) p0 + len0) - start_ptr;
	appendBinaryStringInfo(&str, start_ptr, chunk_len);

	text_position_cleanup(&state);

	ret_text = PG_STR_GET_TEXT(str.data);
	pfree(str.data);
	if(tofree0)
		pfree(tofree0);
	if(tofree1)
		pfree(tofree1);
	if(tofree2)
		pfree(tofree2);

	PG_RETURN_TEXT_P(ret_text);
}

/*
 * check_replace_text_has_escape_char
 *
 * check whether replace_text contains escape char.
 */
static bool
check_replace_text_has_escape_char(const text *replace_text)
{
	const char *p = VARDATA_ANY((void *) replace_text);
	const char *p_end = p + VARSIZE_ANY((void *) replace_text);

	if (pg_database_encoding_max_length() == 1)
	{
		for (; p < p_end; p++)
		{
			if (*p == '\\')
				return true;
		}
	}
	else
	{
		for (; p < p_end; p += pg_mblen(p))
		{
			if (*p == '\\')
				return true;
		}
	}

	return false;
}

/*
 * appendStringInfoRegexpSubstr
 *
 * Append replace_text to str, substituting regexp back references for
 * \n escapes.	start_ptr is the start of the match in the source string,
 * at logical character position data_pos.
 */
static void
appendStringInfoRegexpSubstr(StringInfo str, text *replace_text,
							 regmatch_t *pmatch,
							 char *start_ptr, int data_pos)
{
	const char *p = VARDATA_ANY(replace_text);
	const char *p_end = p + VARSIZE_ANY_EXHDR(replace_text);
	int			eml = pg_database_encoding_max_length();

	for (;;)
	{
		const char *chunk_start = p;
		int			so;
		int			eo;

		/* Find next escape char. */
		if (eml == 1)
		{
			for (; p < p_end && *p != '\\'; p++)
				 /* nothing */ ;
		}
		else
		{
			for (; p < p_end && *p != '\\'; p += pg_mblen(p))
				 /* nothing */ ;
		}

		/* Copy the text we just scanned over, if any. */
		if (p > chunk_start)
			appendBinaryStringInfo(str, chunk_start, p - chunk_start);

		/* Done if at end of string, else advance over escape char. */
		if (p >= p_end)
			break;
		p++;

		if (p >= p_end)
		{
			/* Escape at very end of input.  Treat same as unexpected char */
			appendStringInfoChar(str, '\\');
			break;
		}

		if (*p >= '1' && *p <= '9')
		{
			/* Use the back reference of regexp. */
			int			idx = *p - '0';

			so = pmatch[idx].rm_so;
			eo = pmatch[idx].rm_eo;
			p++;
		}
		else if (*p == '&')
		{
			/* Use the entire matched string. */
			so = pmatch[0].rm_so;
			eo = pmatch[0].rm_eo;
			p++;
		}
		else if (*p == '\\')
		{
			/* \\ means transfer one \ to output. */
			appendStringInfoChar(str, '\\');
			p++;
			continue;
		}
		else
		{
			/*
			 * If escape char is not followed by any expected char, just treat
			 * it as ordinary data to copy.  (XXX would it be better to throw
			 * an error?)
			 */
			appendStringInfoChar(str, '\\');
			continue;
		}

		if (so != -1 && eo != -1)
		{
			/*
			 * Copy the text that is back reference of regexp.	Note so and eo
			 * are counted in characters not bytes.
			 */
			char	   *chunk_start;
			int			chunk_len;

			Assert(so >= data_pos);
			chunk_start = start_ptr;
			chunk_start += charlen_to_bytelen(chunk_start, so - data_pos);
			chunk_len = charlen_to_bytelen(chunk_start, eo - so);
			appendBinaryStringInfo(str, chunk_start, chunk_len);
		}
	}
}

#define REGEXP_REPLACE_BACKREF_CNT		10

/*
 * replace_text_regexp
 *
 * replace text that matches to regexp in src_text to replace_text.
 *
 * Note: to avoid having to include regex.h in builtins.h, we declare
 * the regexp argument as void *, but really it's regex_t *.
 */
text *
replace_text_regexp(text *src_text, void *regexp,
					text *replace_text, bool glob)
{
	text	   *ret_text;
	regex_t    *re = (regex_t *) regexp;
	int			src_text_len = VARSIZE_ANY_EXHDR(src_text);
	StringInfoData buf;
	regmatch_t	pmatch[REGEXP_REPLACE_BACKREF_CNT];
	pg_wchar   *data;
	size_t		data_len;
	int			search_start;
	int			data_pos;
	char	   *start_ptr;
	bool		have_escape;

	initStringInfo(&buf);

	/* Convert data string to wide characters. */
	data = (pg_wchar *) palloc((src_text_len + 1) * sizeof(pg_wchar));
	data_len = pg_mb2wchar_with_len(VARDATA_ANY(src_text), data, src_text_len);

	/* Check whether replace_text has escape char. */
	have_escape = check_replace_text_has_escape_char(replace_text);

	/* start_ptr points to the data_pos'th character of src_text */
	start_ptr = (char *) VARDATA_ANY(src_text);
	data_pos = 0;

	search_start = 0;
	while (search_start <= data_len)
	{
		int			regexec_result;

		CHECK_FOR_INTERRUPTS();

		regexec_result = pg_regexec(re,
									data,
									data_len,
									search_start,
									NULL,		/* no details */
									REGEXP_REPLACE_BACKREF_CNT,
									pmatch,
									0);

		if (regexec_result == REG_NOMATCH)
			break;

		if (regexec_result != REG_OKAY)
		{
			char		errMsg[100];

			pg_regerror(regexec_result, re, errMsg, sizeof(errMsg));
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
					 errmsg("regular expression failed: %s", errMsg),
							 errOmitLocation(true)));
		}

		/*
		 * Copy the text to the left of the match position.  Note we are given
		 * character not byte indexes.
		 */
		if (pmatch[0].rm_so - data_pos > 0)
		{
			int			chunk_len;

			chunk_len = charlen_to_bytelen(start_ptr,
										   pmatch[0].rm_so - data_pos);
			appendBinaryStringInfo(&buf, start_ptr, chunk_len);
			/*
			 * Advance start_ptr over that text, to avoid multiple rescans of
			 * it if the replace_text contains multiple back-references.
			 */
			start_ptr += chunk_len;
			data_pos = pmatch[0].rm_so;
		}

		/*
		 * Copy the replace_text. Process back references when the
		 * replace_text has escape characters.
		 */
		if (have_escape)
			appendStringInfoRegexpSubstr(&buf, replace_text, pmatch,
										 start_ptr, data_pos);
		else
			appendStringInfoText(&buf, replace_text);

		/* Advance start_ptr and data_pos over the matched text. */
		start_ptr += charlen_to_bytelen(start_ptr,
										pmatch[0].rm_eo - data_pos);
		data_pos = pmatch[0].rm_eo;

		/*
		 * When global option is off, replace the first instance only.
		 */
		if (!glob)
			break;

		/*
		 * Search from next character when the matching text is zero width.
		 */
		search_start = data_pos;
		if (pmatch[0].rm_so == pmatch[0].rm_eo)
			search_start++;
	}

	/*
	 * Copy the text to the right of the last match.
	 */
	if (data_pos < data_len)
	{
		int			chunk_len;

		chunk_len = ((char *) src_text + VARSIZE_ANY(src_text)) - start_ptr;
		appendBinaryStringInfo(&buf, start_ptr, chunk_len);
	}

	ret_text = cstring_to_text_with_len(buf.data, buf.len);
	pfree(buf.data);
	pfree(data);

	return ret_text;
}

/*
 * split_text
 * parse input string
 * return ord item (1 based)
 * based on provided field separator
 */
Datum
split_text(PG_FUNCTION_ARGS)
{
	Datum d0 = PG_GETARG_DATUM(0);
	char *p0; void *tofree0; int len0;

	Datum d1 = PG_GETARG_DATUM(1);
	char *p1; void *tofree1; int len1;

	int			fldnum = PG_GETARG_INT32(2);

	int			inputstring_len; 
	int			fldsep_len; 
	TextPositionState state = 		
		{
		0, /* use_wchar */
		NULL, /* str1 */
		NULL, /* str2 */
		NULL, /* wstr1 */
		NULL, /* wstr2 */
		0, /* len1 */
		0, /* len2 */
		};

	int			start_posn;
	int			end_posn;
	text	   *result_text;

	varattrib_untoast_ptr_len(d0, &p0, &len0, &tofree0);
	varattrib_untoast_ptr_len(d1, &p1, &len1, &tofree1);

	if(pg_database_encoding_max_length() == 1)
	{
		inputstring_len = len0;
		fldsep_len = len1;
	}
	else
	{
		inputstring_len = pg_mbstrlen_with_len(p0, len0);
		fldsep_len = pg_mbstrlen_with_len(p1, len1);
	}

	/* field number is 1 based */
	if (fldnum < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("field position must be greater than zero"),
						 errOmitLocation(true)));

	/* return empty string for empty input string */
	if (inputstring_len < 1)
	{
		if(tofree0)
			pfree(tofree0);
		if(tofree1)
			pfree(tofree1);

		PG_RETURN_TEXT_P(PG_STR_GET_TEXT(""));
	}

	/* empty field separator */
	if (fldsep_len < 1)
	{
		if(tofree0)
			pfree(tofree0);
		if(tofree1)
			pfree(tofree1);

		/* if first field, return input string, else empty string */
		if (fldnum == 1)
			return d0;
		else
			PG_RETURN_TEXT_P(PG_STR_GET_TEXT(""));
	}

	text_position_setup_ptr_len(p0, len0, p1, len1, &state);

	/* identify bounds of first field */
	start_posn = 1;
	end_posn = text_position_next(1, &state);

	/* special case if fldsep not found at all */
	if (end_posn == 0)
	{
		text_position_cleanup(&state);
		if(tofree0)
			pfree(tofree0);
		if(tofree1)
			pfree(tofree1);

		/* if field 1 requested, return input string, else empty string */
		if (fldnum == 1)
			return d0;
		else
			PG_RETURN_TEXT_P(cstring_to_text(""));
	}

	while (end_posn > 0 && --fldnum > 0)
	{
		/* identify bounds of next field */
		start_posn = end_posn + fldsep_len;
		end_posn = text_position_next(start_posn, &state);
	}

	text_position_cleanup(&state);

	if (fldnum > 0)
	{
		/* N'th field separator not found */
		/* if last field requested, return it, else empty string */
		if (fldnum == 1)
			result_text = text_substring(d0, 
										 start_posn,
										 -1,
										 true);
		else
			result_text = cstring_to_text("");
	}
	else
	{
		/* non-last field requested */
		result_text = text_substring(d0,
									 start_posn,
									 end_posn - start_posn,
									 false);
	}

	if(tofree0)
		pfree(tofree0);
	if(tofree1)
		pfree(tofree1);

	PG_RETURN_TEXT_P(result_text);
}


/*
 * text_to_array_impl
 *		Carries out the actual tokenization and array conversion of an input string.
 *
 * Parameters:
 * 		string: Where to start in the input string
 * 		stringByteLen: Length of current string
 * 		delimiter: Which delimiter to use
 * 		delimiterByteLen: Length of delimiter in bytes
 * 		delimiterCharLen: Length of delimiter in chars
 * 		arrayState: State of the output array where we accumulate results
 * 		endOfString: Do we expect any more chunk of the main input string?
 *
 * Returns the pointer where the last match was found. Successively the
 * caller can splice more data starting from this address to find further
 * array elements.
 */
static char* text_to_array_impl(char *string, int stringByteLen, char *delimiter,
		int delimiterByteLen, int delimiterCharLen, ArrayBuildState **arrayState, bool endOfString)
{
	int start_posn = 1;
	int fldnum = 1;
	int end_posn = 0;
	int chunk_len = 0;
	text	   *result_text;

	char* cur_ptr = string;

	TextPositionState state =
		{
		0, /* use_wchar */
		NULL, /* str1 */
		NULL, /* str2 */
		NULL, /* wstr1 */
		NULL, /* wstr2 */
		0, /* len1 */
		0, /* len2 */
		};

	text_position_setup_ptr_len(string, stringByteLen, delimiter, delimiterByteLen, &state);

	for (fldnum = 1;; fldnum++) /* field number is 1 based */
	{
		end_posn = text_position_next(start_posn, &state);

		if (end_posn == 0 && !endOfString)
		{
			break;
		}
		else if (end_posn == 0)
		{
			/* fetch last field */
			chunk_len = (string + stringByteLen) - cur_ptr;
		}
		else
		{
			/* fetch non-last field */
			chunk_len = charlen_to_bytelen(cur_ptr, end_posn - start_posn);
		}

		/* must build a temp text datum to pass to accumArrayResult */
		result_text = cstring_to_text_with_len(cur_ptr, chunk_len);

		/* stash away this field */
		*arrayState = accumArrayResult(*arrayState,
								  PointerGetDatum(result_text),
								  false,
								  TEXTOID,
								  CurrentMemoryContext);

		pfree(result_text);

		if (end_posn == 0)
		{
			/* Process next sub-string if any */
			break;
		}

		start_posn = end_posn;
		cur_ptr += chunk_len;
		start_posn += delimiterCharLen;
		cur_ptr += charlen_to_bytelen(cur_ptr, delimiterCharLen);
	}

	text_position_cleanup(&state);

	return cur_ptr;
}


/*
 * text_to_array_multi_pass
 *		Carries out the actual tokenization and array conversion of input string
 *		in multiple passes, where each pass is restricted to GPDB memory allocation limit.
 *
 * Parameters:
 * 		string: The start of the input string
 * 		stringByteLen: Length of current string
 * 		delimiter: Which delimiter to use
 * 		delimiterByteLen: Length of delimiter in bytes
 * 		delimiterCharLen: Length of delimiter in chars
 * 		endOfString: Do we expect any more chunk of the main input string?
 *
 * Returns the ArrayBuildState containing all the array elements.
 */
static ArrayBuildState* text_to_array_multi_pass(char *string, int stringByteLen, char *delimiter, int delimiterByteLen, int delimiterCharLen)
{
	ArrayBuildState *astate = NULL;

	/* Start with full string. If it is too big then we chunk it later */
	char	   *start_ptr = string;
	int curSubStringByteLen = stringByteLen;

	bool endOfString = false;

	/* More bytes to consider? */
	while (!endOfString)
	{
		/*
		 * Give the rest of the string to the current pass; may be chunked if
		 * the rest still doesn't fit in the memory
		 */
		curSubStringByteLen = (string + stringByteLen) - start_ptr;

		/* Will this MBCS become too big to fit in memory once converted to wchar? */
		if (pg_database_encoding_max_length() > 1 && curSubStringByteLen > ((MAX_STRING_BYTES)/ sizeof(pg_wchar)))
		{
			int curSubStringCharLen = 0;
			/* We need multi-pass. So find the sub-string boundary for the current pass */
			find_memory_limited_substring(start_ptr, string + stringByteLen - start_ptr,
				(MAX_STRING_BYTES) / sizeof(pg_wchar), &curSubStringByteLen, &curSubStringCharLen);
		}

		Insist(start_ptr + curSubStringByteLen <= string + stringByteLen);

		endOfString = ((start_ptr + curSubStringByteLen) == (string + stringByteLen));

		char *nextStartPtr = text_to_array_impl(start_ptr, curSubStringByteLen, delimiter, delimiterByteLen, delimiterCharLen, &astate, endOfString);

		Insist(nextStartPtr >= start_ptr);

		if (!endOfString && nextStartPtr == start_ptr)
		{
			elog(ERROR, "String size not supported.");
		}

		start_ptr = nextStartPtr;
	}

	return astate;
}


/*
 *  * text_to_array
 *   * parse input string
 *    * return text array of elements
 *     * based on provided field separator
 *      */
Datum
text_to_array(PG_FUNCTION_ARGS)
{
	Datum stringDatum = PG_GETARG_DATUM(0);
	char *string = NULL;
	void *toFreeString = NULL;
	int stringByteLen = 0;

	Datum delimiterDatum = PG_GETARG_DATUM(1);
	char *delimiter = NULL;
	void *toFreeDelimiter = NULL;
	int delimiterByteLen = 0;

	int stringCharLen = 0;
	int	delimiterCharLen = 0;

	varattrib_untoast_ptr_len(stringDatum, &string, &stringByteLen, &toFreeString);
	varattrib_untoast_ptr_len(delimiterDatum, &delimiter, &delimiterByteLen, &toFreeDelimiter);

	if(pg_database_encoding_max_length() == 1)
	{
		stringCharLen = stringByteLen;
		delimiterCharLen = delimiterByteLen;
	}
	else
	{
		stringCharLen = pg_mbstrlen_with_len(string, stringByteLen);
		delimiterCharLen = pg_mbstrlen_with_len(delimiter, delimiterByteLen);
	}

	/* return NULL for empty input string */
	if (stringCharLen < 1)
	{
		if(toFreeString)
		{
			pfree(toFreeString);
		}

		if(toFreeDelimiter)
		{
			pfree(toFreeDelimiter);
		}

		PG_RETURN_NULL();
	}

	/*
	 * empty field separator return one element, 1D, array using the input
	 * string
	 */
	if (delimiterCharLen < 1)
	{
		if(toFreeString)
		{
			pfree(toFreeString);
		}

		if(toFreeDelimiter)
		{
			pfree(toFreeDelimiter);
		}

		PG_RETURN_ARRAYTYPE_P(create_singleton_array(fcinfo, TEXTOID, stringDatum, 1));
	}

	ArrayBuildState *astate = text_to_array_multi_pass(string, stringByteLen, delimiter, delimiterByteLen, delimiterCharLen);

	if(toFreeString)
	{
		pfree(toFreeString);
	}
	if(toFreeDelimiter)
	{
		pfree(toFreeDelimiter);
	}

	PG_RETURN_DATUM(makeArrayResult(astate, CurrentMemoryContext));
}

/*
 * array_to_text
 * concatenate Cstring representation of input array elements
 * using provided field separator
 */
Datum
array_to_text(PG_FUNCTION_ARGS)
{
	ArrayType  *v = PG_GETARG_ARRAYTYPE_P(0);
	char	   *fldsep = text_to_cstring(PG_GETARG_TEXT_PP(1));
	int			nitems,
			   *dims,
				ndims;
	Oid			element_type;
	int			typlen;
	bool		typbyval;
	char		typalign;
	StringInfoData buf;
	bool		printed = false;
	char	   *p;
	bits8	   *bitmap;
	int			bitmask;
	int			i;
	ArrayMetaState *my_extra;

	ndims = ARR_NDIM(v);
	dims = ARR_DIMS(v);
	nitems = ArrayGetNItems(ndims, dims);

	/* if there are no elements, return an empty string */
	if (nitems == 0)
		PG_RETURN_TEXT_P(cstring_to_text(""));

	element_type = ARR_ELEMTYPE(v);
	initStringInfo(&buf);

	/*
	 * We arrange to look up info about element type, including its output
	 * conversion proc, only once per series of calls, assuming the element
	 * type doesn't change underneath us.
	 */
	my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL)
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(ArrayMetaState));
		my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
		my_extra->element_type = ~element_type;
	}

	if (my_extra->element_type != element_type)
	{
		/*
		 * Get info about element type, including its output conversion proc
		 */
		get_type_io_data(element_type, IOFunc_output,
						 &my_extra->typlen, &my_extra->typbyval,
						 &my_extra->typalign, &my_extra->typdelim,
						 &my_extra->typioparam, &my_extra->typiofunc);
		fmgr_info_cxt(my_extra->typiofunc, &my_extra->proc,
					  fcinfo->flinfo->fn_mcxt);
		my_extra->element_type = element_type;
	}
	typlen = my_extra->typlen;
	typbyval = my_extra->typbyval;
	typalign = my_extra->typalign;

	p = ARR_DATA_PTR(v);
	bitmap = ARR_NULLBITMAP(v);
	bitmask = 1;

	for (i = 0; i < nitems; i++)
	{
		Datum		itemvalue;
		char	   *value;

		/* Get source element, checking for NULL */
		if (bitmap && (*bitmap & bitmask) == 0)
		{
			/* we ignore nulls */
		}
		else
		{
			itemvalue = fetch_att(p, typbyval, typlen);

			value = OutputFunctionCall(&my_extra->proc, itemvalue);

			if (printed)
				appendStringInfo(&buf, "%s%s", fldsep, value);
			else
				appendStringInfoString(&buf, value);
			printed = true;

			p = att_addlength_pointer(p, typlen, p);
			p = (char *) att_align_nominal(p, typalign);
		}

		/* advance bitmap pointer if any */
		if (bitmap)
		{
			bitmask <<= 1;
			if (bitmask == 0x100)
			{
				bitmap++;
				bitmask = 1;
			}
		}
	}

	PG_RETURN_TEXT_P(cstring_to_text_with_len(buf.data, buf.len));
}

#define HEXBASE 16
/*
 * Convert a int32 to a string containing a base 16 (hex) representation of
 * the number.
 */
Datum
to_hex32(PG_FUNCTION_ARGS)
{
	uint32		value = (uint32) PG_GETARG_INT32(0);
	char	   *ptr;
	const char *digits = "0123456789abcdef";
	char		buf[32];		/* bigger than needed, but reasonable */

	ptr = buf + sizeof(buf) - 1;
	*ptr = '\0';

	do
	{
		*--ptr = digits[value % HEXBASE];
		value /= HEXBASE;
	} while (ptr > buf && value);

	PG_RETURN_TEXT_P(cstring_to_text(ptr));
}

/*
 * Convert a int64 to a string containing a base 16 (hex) representation of
 * the number.
 */
Datum
to_hex64(PG_FUNCTION_ARGS)
{
	uint64		value = (uint64) PG_GETARG_INT64(0);
	char	   *ptr;
	const char *digits = "0123456789abcdef";
	char		buf[32];		/* bigger than needed, but reasonable */

	ptr = buf + sizeof(buf) - 1;
	*ptr = '\0';

	do
	{
		*--ptr = digits[value % HEXBASE];
		value /= HEXBASE;
	} while (ptr > buf && value);

	PG_RETURN_TEXT_P(cstring_to_text(ptr));
}

/*
 * Create an md5 hash of a text string and return it as hex
 *
 * md5 produces a 16 byte (128 bit) hash; double it for hex
 */
#define MD5_HASH_LEN  32

Datum
md5_text(PG_FUNCTION_ARGS)
{
	text	   *in_text = PG_GETARG_TEXT_PP(0);
	size_t		len;
	char		hexsum[MD5_HASH_LEN + 1];

	/* Calculate the length of the buffer using varlena metadata */
	len = VARSIZE_ANY_EXHDR(in_text);

	/* get the hash result */
	if (pg_md5_hash(VARDATA_ANY(in_text), len, hexsum) == false)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	/* convert to text and return it */
	PG_RETURN_TEXT_P(cstring_to_text(hexsum));
}

/*
 * Create an md5 hash of a bytea field and return it as a hex string:
 * 16-byte md5 digest is represented in 32 hex characters.
 */
Datum
md5_bytea(PG_FUNCTION_ARGS)
{
	bytea	   *in = PG_GETARG_BYTEA_PP(0);
	size_t		len;
	char		hexsum[MD5_HASH_LEN + 1];

	len = VARSIZE_ANY_EXHDR(in);
	if (pg_md5_hash(VARDATA_ANY(in), len, hexsum) == false)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	PG_RETURN_TEXT_P(cstring_to_text(hexsum));
}

/*
 * Return the size of a datum, possibly compressed
 *
 * Works on any data type
 */
Datum
pg_column_size(PG_FUNCTION_ARGS)
{
	Datum		value = PG_GETARG_DATUM(0);
	int32		result;
	int			typlen;

	/* On first call, get the input type's typlen, and save at *fn_extra */
	if (fcinfo->flinfo->fn_extra == NULL)
	{
		/* Lookup the datatype of the supplied argument */
		Oid			argtypeid = get_fn_expr_argtype(fcinfo->flinfo, 0);

		typlen = get_typlen(argtypeid);
		if (typlen == 0)		/* should not happen */
			elog(ERROR, "cache lookup failed for type %u", argtypeid);

		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(int));
		*((int *) fcinfo->flinfo->fn_extra) = typlen;
	}
	else
		typlen = *((int *) fcinfo->flinfo->fn_extra);

	if (typlen == -1)
	{
		/* varlena type, possibly toasted */
		result = toast_datum_size(value);
	}
	else if (typlen == -2)
	{
		/* cstring */
		result = strlen(DatumGetCString(value)) + 1;
	}
	else
	{
		/* ordinary fixed-width type */
		result = typlen;
	}

	PG_RETURN_INT32(result);
}


/*
 * string_agg - Concatenates values and returns string.
 *
 * Syntax: string_agg(value text, delimiter text = '') RETURNS text
 *
 * Note: Any NULL values are ignored. The first-call delimiter isn't
 * actually used at all, and on subsequent calls the delimiter precedes
 * the associated value.
 */

/* subroutine to initialize state */
static StringInfo
makeStringAggState(FunctionCallInfo fcinfo)
{
	StringInfo	state;
	MemoryContext aggcontext;
	MemoryContext oldcontext;

	if (!(fcinfo->context && IsA(fcinfo->context, AggState)))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "string_agg_transfn called in non-aggregate context");
	}
	aggcontext = ((AggState*)fcinfo->context)->aggcontext;

	/*
	 * Create state in aggregate context.  It'll stay there across subsequent
	 * calls.
	 */
	oldcontext = MemoryContextSwitchTo(aggcontext);
	state = makeStringInfo();
	MemoryContextSwitchTo(oldcontext);

	return state;
}

Datum
string_agg_transfn(PG_FUNCTION_ARGS)
{
	StringInfo	state;

	state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);

	/* Append the element unless null. */
	if (!PG_ARGISNULL(1))
	{
		if (state == NULL)
			state = makeStringAggState(fcinfo);
		appendStringInfoText(state, PG_GETARG_TEXT_PP(1));		/* value */
	}

	/*
	 * The transition type for string_agg() is declared to be "internal",
	 * which is a pass-by-value type the same size as a pointer.
	 */
	PG_RETURN_POINTER(state);
}

Datum
string_agg_delim_transfn(PG_FUNCTION_ARGS)
{
	StringInfo	state;

	state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);

	/* Append the value unless null. */
	if (!PG_ARGISNULL(1))
	{
		/* On the first time through, we ignore the delimiter. */
		if (state == NULL)
			state = makeStringAggState(fcinfo);
		else if (!PG_ARGISNULL(2))
			appendStringInfoText(state, PG_GETARG_TEXT_PP(2));	/* delimiter */

		appendStringInfoText(state, PG_GETARG_TEXT_PP(1));		/* value */
	}

	/*
	 * The transition type for string_agg() is declared to be "internal",
	 * which is a pass-by-value type the same size as a pointer.
	 */
	PG_RETURN_POINTER(state);
}

Datum
string_agg_finalfn(PG_FUNCTION_ARGS)
{
	StringInfo	state;

	/* cannot be called directly because of internal-type argument */
	if (!(fcinfo->context && IsA(fcinfo->context, AggState)))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "string_agg_finalfn called in non-aggregate context");
	}

	state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);

	if (state != NULL)
		PG_RETURN_TEXT_P(cstring_to_text(state->data));
	else
		PG_RETURN_NULL();
}

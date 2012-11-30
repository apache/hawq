/*
 * Utility for inspecting binary files of Greenplum Database, derived from
 * gp_filedump from Red Hat. Original head follows
 */
/*
 * gp_filedump.c - PostgreSQL file dump utility for dumping and
 *                 formatting heap(data), index and control files.
 *
 * Copyright (c) 2008-2010 Greenplum, Inc.
 * Copyright (c) 2002-2007 Red Hat, Inc. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Author: Patrick Macdonald <patrickm@redhat.com>
 *         Gavin Sherry <gsherry@greenplum.com>
 *
 */

#include "gp_filedump.h"

bool            assert_enabled = true;
/* Global variables for ease of use mostly */
static FILE    *fp = NULL;	/* File to dump or format */
static char    *fileName = NULL;/* File name for display */
static char    *buffer = NULL;	/* Cache for current block */
static unsigned int blockSize = 0;	/* Current block size */
static unsigned int currentBlock = 0;	/* Current block in file */
static unsigned int pageOffset = 0;	/* Offset of current block */
static unsigned int bytesToFormat = 0;	/* Number of bytes to format */
static unsigned int blockVersion = 0;	/* Block version number */
static char    *data_dir = NULL;/* data directory */
static char    *commit_log_dir = NULL;	/* clog */
Oid             relid = InvalidOid;
TupleDesc       tupdesc = NULL;
MemoryContext   CurrentMemoryContext = NULL;


/* Function Prototypes */
static void     DisplayOptions(unsigned int validOptions);
static unsigned int ConsumeOptions(int numOptions, char **options);
static int      GetOptionValue(char *optionString);
static void     FormatBlock();
static unsigned int GetBlockSize();
static unsigned int GetSpecialSectionType(Page page);
static bool     IsBtreeMetaPage(Page page);
static void     CreateDumpFileHeader(int numOptions, char **options);
static int      FormatHeader(Page page);
static void     FormatItemBlock(Page page);
static void
FormatItem(unsigned int numBytes, unsigned int startIndex,
	   unsigned int formatAs, bool deparse);
static void     FormatSpecial();
static void     FormatControl();
static void     FormatBinary(unsigned int numBytes, unsigned int startIndex);
static void     DumpBinaryBlock();
static void     DumpFileContents();
static void     build_tupdesc(void);

void           *
MemoryContextAllocZeroImpl(MemoryContext context, Size size, const char *sfile, const char *sfunc, int sline)
{
	void           *ptr = malloc(size);
	memset(ptr, 0, size);

	return ptr;
}

void
MemoryContextFreeImpl(void *pointer, const char *sfile, const char *sfunc, int sline)
{
	free(pointer);
}

void           *
MemoryContextAllocImpl(MemoryContext context, Size size, const char *sfile, const char *sfunc, int sline)
{
	return malloc(size);
}

Datum
toast_flatten_tuple_attribute(Datum value,
			      Oid typeId, int32 typeMod)
{
	return PointerGetDatum(NULL);
}

int
ExceptionalCondition(const char *conditionName,
		     const char *errorType,
		     const char *fileName,
		     int lineNumber)
{
	fprintf(stderr, "TRAP: %s(\"%s\", File: \"%s\", Line: %d)\n",
		errorType, conditionName,
		fileName, lineNumber);
	abort();
	return 0;
}

int
errcode_for_file_access(void)
{
	return 0;
}

bool
errstart(int elevel, const char *filename, int lineno,
		 const char *funcname, const char* domain)
{
	return (elevel >= ERROR);
}

void
errfinish(int dummy,...)
{
	exit(1);
}

void
elog_start(const char *filename, int lineno, const char *funcname)
{
}

void
elog_finish(int elevel, const char *fmt,...)
{
	fprintf(stderr, "ERROR: %s\n", fmt);
	exit(1);
}

int
errcode(int sqlerrcode)
{
	return 0;		/* return value does not matter */
}

int
errmsg(const char *fmt,...)
{
	fprintf(stderr, "ERROR: %s\n", fmt);
	return 0;		/* return value does not matter */
}

int
errmsg_internal(const char *fmt,...)
{
	fprintf(stderr, "ERROR: %s\n", fmt);
	return 0;		/* return value does not matter */
}

int
errdetail(const char *fmt,...)
{
	fprintf(stderr, "DETAIL: %s\n", fmt);
	return 0;		/* return value does not matter */
}

int
errdetail_log(const char *fmt,...)
{
	fprintf(stderr, "DETAIL: %s\n", fmt);
	return 0;		/* return value does not matter */
}

int
errhint(const char *fmt,...)
{
	fprintf(stderr, "HINT: %s\n", fmt);
	return 0;		/* return value does not matter */
}

int
errOmitLocation(bool omitLocation)
{
	return 0;
}

#define	NUMERIC_LOCAL_NDIG	36	/* number of 'digits' in local
					 * digits[] */
#define NUMERIC_LOCAL_NMAX	(NUMERIC_LOCAL_NDIG - 2)
#define	NUMERIC_LOCAL_DTXT	128	/* number of char in local text */
#define NUMERIC_LOCAL_DMAX	(NUMERIC_LOCAL_DTXT - 2)

#define NUMERIC_SIGN_MASK	0xC000
#define NUMERIC_POS			0x0000
#define NUMERIC_NEG			0x4000
#define NUMERIC_NAN			0xC000
#define NUMERIC_DSCALE_MASK 0x3FFF
#define NUMERIC_SIGN(n)		((n)->n_sign_dscale & NUMERIC_SIGN_MASK)
#define NUMERIC_DSCALE(n)	((n)->n_sign_dscale & NUMERIC_DSCALE_MASK)
#define NUMERIC_IS_NAN(n)	(NUMERIC_SIGN(n) != NUMERIC_POS &&	\
							 NUMERIC_SIGN(n) != NUMERIC_NEG)
#define quick_init_var(v) \
	do { \
		(v)->buf = (v)->ndb;	\
		(v)->digits = NULL; 	\
	} while (0)


#define init_var(v) \
	do { \
		quick_init_var((v));	\
		(v)->ndigits = (v)->weight = (v)->sign = (v)->dscale = 0; \
	} while (0)


#define digitbuf_alloc(ndigits)  \
	((NumericDigit *) malloc((ndigits) * sizeof(NumericDigit)))

#define digitbuf_free(v)	\
	do { \
		if ((v)->buf != (v)->ndb)	\
		{							\
		 	free((v)->buf); 		\
			(v)->buf = (v)->ndb;	\
		}	\
	} while (0)

#define free_var(v)	\
				digitbuf_free((v));

/*
 * init_alloc_var() -
 *
 *	Init a var and allocate digit buffer of ndigits digits (plus a spare
 *  digit for rounding).
 *  Called when first using a var.
 */
#define	init_alloc_var(v, n) \
	do 	{	\
		(v)->buf = (v)->ndb;	\
		(v)->ndigits = (n);	\
		if ((n) > NUMERIC_LOCAL_NMAX)	\
			(v)->buf = digitbuf_alloc((n) + 1);	\
		(v)->buf[0] = 0;	\
		(v)->digits = (v)->buf + 1;	\
	} while (0)
#define NUMERIC_HDRSZ	(VARHDRSZ + sizeof(int16) + sizeof(uint16))
#define NUMERIC_SIGN_DSCALE(num) ((num)->n_sign_dscale)
#define NUMERIC_WEIGHT(num) ((num)->n_weight)
#define NUMERIC_DIGITS(num) ((NumericDigit *)(num)->n_data)
#define NUMERIC_NDIGITS(num) \
	((VARSIZE(num) - NUMERIC_HDRSZ) / sizeof(NumericDigit))

#define NBASE		10000
#define HALF_NBASE	5000
#define DEC_DIGITS	4	/* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS	2	/* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS	4

#if DEC_DIGITS == 4
static const int round_powers[4] = {0, 1000, 100, 10};
#endif

typedef int16   NumericDigit;
typedef struct NumericData
{
	int32           vl_len_;/* varlena header (do not touch directly!) */
	int16           n_weight;	/* Weight of 1st digit	 */
	uint16          n_sign_dscale;	/* Sign + display scale */
	char            n_data[1];	/* Digits (really array of
					 * NumericDigit) */
}               NumericData;

typedef NumericData *Numeric;

typedef struct NumericVar
{
	int             ndigits;/* # of digits in digits[] - can be 0! */
	int             weight;	/* weight of first digit */
	int             sign;	/* NUMERIC_POS, NUMERIC_NEG, or NUMERIC_NAN */
	int             dscale;	/* display scale */
	NumericDigit   *buf /* start of space for digits[] */ ;
	NumericDigit   *digits;	/* base-NBASE digits */
	NumericDigit    ndb[NUMERIC_LOCAL_NDIG];	/* local space for
							 * digits[] */
}               NumericVar;

static void
round_var(NumericVar * var, int rscale)
{
	NumericDigit   *digits = var->digits;
	int             di;
	int             ndigits;
	int             carry;

	var->dscale = rscale;

	/* decimal digits wanted */
	di = (var->weight + 1) * DEC_DIGITS + rscale;

	/*
	 * If di = 0, the value loses all digits, but could round up to 1 if its
	 * first extra digit is >= 5.  If di < 0 the result must be 0.
	 */
	if (di < 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		var->sign = NUMERIC_POS;
	}
	else
	{
		/* NBASE digits wanted */
		ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

		/* 0, or number of decimal digits to keep in last NBASE digit */
		di %= DEC_DIGITS;

		if (ndigits < var->ndigits ||
		    (ndigits == var->ndigits && di > 0))
		{
			var->ndigits = ndigits;

#if DEC_DIGITS == 1
			/* di must be zero */
			carry = (digits[ndigits] >= HALF_NBASE) ? 1 : 0;
#else
			if (di == 0)
				carry = (digits[ndigits] >= HALF_NBASE) ? 1 : 0;
			else
			{
				/* Must round within last NBASE digit */
				int             extra, pow10;

#if DEC_DIGITS == 4
				pow10 = round_powers[di];
#elif DEC_DIGITS == 2
				pow10 = 10;
#else
#error unsupported NBASE
#endif
				extra = digits[--ndigits] % pow10;
				digits[ndigits] -= extra;
				carry = 0;
				if (extra >= pow10 / 2)
				{
					pow10 += digits[ndigits];
					if (pow10 >= NBASE)
					{
						pow10 -= NBASE;
						carry = 1;
					}
					digits[ndigits] = pow10;
				}
			}
#endif

			/* Propagate carry if needed */
			while (carry)
			{
				carry += digits[--ndigits];
				if (carry >= NBASE)
				{
					digits[ndigits] = carry - NBASE;
					carry = 1;
				}
				else
				{
					digits[ndigits] = carry;
					carry = 0;
				}
			}

			if (ndigits < 0)
			{
				Assert(ndigits == -1);	/* better not have added
							 * > 1 digit */
				Assert(var->digits > var->buf);
				var->digits--;
				var->ndigits++;
				var->weight++;
			}
		}
	}
}

static void
init_var_from_num(Numeric num, NumericVar * dest)
{
	int             ndigits;

	ndigits = NUMERIC_NDIGITS(num);

	init_alloc_var(dest, ndigits);

	dest->weight = NUMERIC_WEIGHT(num);
	dest->sign = NUMERIC_SIGN(num);
	dest->dscale = NUMERIC_DSCALE(num);

	memcpy(dest->digits, NUMERIC_DIGITS(num), ndigits * sizeof(NumericDigit));
}

static char    *
get_str_from_var(NumericVar * var, int dscale)
{
	char           *str;
	char           *cp;
	char           *endcp;
	int             i;
	int             d;
	NumericDigit    dig;

#if DEC_DIGITS > 1
	NumericDigit    d1;
#endif

	if (dscale < 0)
		dscale = 0;

	/*
	 * Check if we must round up before printing the value and do so.
	 */
	round_var(var, dscale);

	/*
	 * Allocate space for the result.
	 *
	 * i is set to to # of decimal digits before decimal point. dscale is the
	 * # of decimal digits we will print after decimal point. We may generate
	 * as many as DEC_DIGITS-1 excess digits at the end, and in addition we
	 * need room for sign, decimal point, null terminator.
	 */
	i = (var->weight + 1) * DEC_DIGITS;
	if (i <= 0)
		i = 1;

	str = palloc(i + dscale + DEC_DIGITS + 2);
	cp = str;

	/*
	 * Output a dash for negative values
	 */
	if (var->sign == NUMERIC_NEG)
		*cp++ = '-';

	/*
	 * Output all digits before the decimal point
	 */
	if (var->weight < 0)
	{
		d = var->weight + 1;
		*cp++ = '0';
	}
	else
	{
		for (d = 0; d <= var->weight; d++)
		{
			dig = (d < var->ndigits) ? var->digits[d] : 0;

			/*
			 * In the first digit, suppress extra leading decimal
			 * zeroes
			 */
#if DEC_DIGITS == 4
			{
				bool            putit = (d > 0);

				d1 = dig / 1000;
				dig -= d1 * 1000;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				d1 = dig / 100;
				dig -= d1 * 100;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				d1 = dig / 10;
				dig -= d1 * 10;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				*cp++ = dig + '0';
			}
#elif DEC_DIGITS == 2
			d1 = dig / 10;
			dig -= d1 * 10;
			if (d1 > 0 || d > 0)
				*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif DEC_DIGITS == 1
			*cp++ = dig + '0';
#else
#error unsupported NBASE
#endif
		}
	}

	/*
	 * If requested, output a decimal point and all the digits that follow it.
	 * We initially put out a multiple of DEC_DIGITS digits, then truncate if
	 * needed.
	 */
	if (dscale > 0)
	{
		*cp++ = '.';
		endcp = cp + dscale;
		for (i = 0; i < dscale; d++, i += DEC_DIGITS)
		{
			dig = (d >= 0 && d < var->ndigits) ? var->digits[d] : 0;
#if DEC_DIGITS == 4
			d1 = dig / 1000;
			dig -= d1 * 1000;
			*cp++ = d1 + '0';
			d1 = dig / 100;
			dig -= d1 * 100;
			*cp++ = d1 + '0';
			d1 = dig / 10;
			dig -= d1 * 10;
			*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif DEC_DIGITS == 2
			d1 = dig / 10;
			dig -= d1 * 10;
			*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif DEC_DIGITS == 1
			*cp++ = dig + '0';
#else
#error unsupported NBASE
#endif
		}
		cp = endcp;
	}
	/*
	 * terminate the string and return it
	 */
	*cp = '\0';
	return str;
}

static char    *
num_out(Numeric num)
{
	NumericVar      x;
	char           *str;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num))
		return (strdup("NaN"));

	/*
	 * Get the number in the variable format.
	 *
	 * Even if we didn't need to change format, we'd still need to copy the
	 * value to have a modifiable copy for rounding.  init_var_from_num() also
	 * guarantees there is extra digit space in case we produce a carry out
	 * from rounding.
	 */
	init_var_from_num(num, &x);

	str = get_str_from_var(&x, x.dscale);

	free_var(&x);

	return (str);
}

/*
 * Given a path to the file we're interested in, find the clog and general
 * data directory.
 */
static void
file_set_dirs(char *file)
{
	int             len = strlen(file);
	int             pos = len;
	int             seps_seen = 0;	/* how man seperators have we seen? */

	for (pos = len - 1; pos >= 0; pos--)
	{
		/* Fix for WIN32 */
		if (file[pos] == '/')
		{
			seps_seen++;
			if (seps_seen == 1)
			{
				/* must be data directory */

				/* could this be a global directory? */
#define GSTR "global/"
#define GSTRLEN strlen(GSTR)
				if (pos + 1 >= GSTRLEN)
				{
					if (strncmp(&(file[pos + 1 - GSTRLEN]), GSTR, GSTRLEN) == 0)
					{
						/* 
						 * It is the global directory, so backup and make the
						 * data directory template0.
						 */
						data_dir = malloc((pos + 1 - GSTRLEN + 6 + 1) *
										  sizeof(char));
						strncpy(data_dir, file, pos + 1 - GSTRLEN);
						strcat(data_dir, "base/1");
					}
				}
				else
				{
					data_dir = malloc((pos + 1) * sizeof(char));
					strncpy(data_dir, file, pos);
				}
				relid = atol(&file[pos + 1]);
			}
			else if (seps_seen == 3)
			{
				/* must be base data directory */
				commit_log_dir = malloc((pos + strlen("/pg_clog") + 1) * sizeof(char));
				strncpy(commit_log_dir, file, pos);
				strcat(commit_log_dir, "/pg_clog");
				break;
			}
		}
	}
	if (data_dir == NULL || commit_log_dir == NULL)
	{
		fprintf(stderr, "could not extract data directories from %s\n", file);
		exit(1);
	}
}

/* Send properly formed usage information to the user.  */
static void
DisplayOptions(unsigned int validOptions)
{
	if (validOptions == OPT_RC_COPYRIGHT)
		printf
		("\nVersion 3.1 (based on Postgres 8.2) Copyright (c) 2002-2007 Red Hat, Inc.\n");

	printf
		("\nUsage: gp_filedump [-abcdfhixy] [-R startblock [endblock]] [-S blocksize] file\n\n"
		 "Display formatted contents of a PostgreSQL heap/index/control file\n"
		 " Defaults are: relative addressing, range of the entire file, block\n"
		 "               size as listed on block 0 in the file\n\n"
	       "The following options are valid for heap and index files:\n"
	  "  -a  Display absolute addresses when formatting (Block header\n"
		 "      information is always block relative)\n"
		 "  -b  Display binary block images within a range (Option will turn\n"
		 "      off all formatting options)\n"
	"  -d  Display formatted block content dump (Option will turn off\n"
		 "      all other formatting options)\n"
		 "  -f  Display formatted block content dump along with interpretation\n"
		 "  -h  Display this information\n"
		 "  -i  Display interpreted item details\n"
	 "  -R  Display specific block ranges within the file (Blocks are\n"
	"      indexed from 0)\n" "        [startblock]: block to start at\n"
		 "        [endblock]: block to end at\n"
		 "      A startblock without an endblock will format the single block\n"
		 "  -S  Force block size to [blocksize]\n"
	"  -x  Force interpreted formatting of block items as index items\n"
	 "  -y  Force interpreted formatting of block items as heap items\n"
		 "  -p  Try and deparse heap data\n"
		 "  -D  Data directory for the segment\n"
		 "  -M  Checksum option for mirror files\n\n"
		 "The following options are valid for control files:\n"
		 "  -c  Interpret the file listed as a control file\n"
	  "  -f  Display formatted content dump along with interpretation\n"
		 "  -S  Force block size to [blocksize]\n"
		 "\nReport bugs to <rhdb@sources.redhat.com>\n");
}

/* Iterate through the provided options and set the option flags. */
/* An error will result in a positive rc and will force a display */
/* of the usage information.  This routine returns enum  */
/* optionReturnCode values. */
static unsigned int
ConsumeOptions(int numOptions, char **options)
{
	unsigned int    rc = OPT_RC_VALID;
	unsigned int    x;
	unsigned int    optionStringLength;
	char           *optionString;
	char            duplicateSwitch = 0x00;

	for (x = 1; x < numOptions; x++)
	{
		optionString = options[x];
		optionStringLength = strlen(optionString);

		/*
		 * Range is a special case where we have to consume the next
		 * 1 or 2
		 */
		/* parameters to mark the range start and end */
		if ((optionStringLength == 2) && (strcmp(optionString, "-R") == 0))
		{
			int             range = 0;

			SET_OPTION(blockOptions, BLOCK_RANGE, 'R');
			/* Only accept the range option once */
			if (rc == OPT_RC_DUPLICATE)
				break;

			/*
			 * Make sure there are options after the range
			 * identifier
			 */
			if (x >= (numOptions - 2))
			{
				rc = OPT_RC_INVALID;
				printf("Error: Missing range start identifier.\n");
				break;
			}
			/*
			 * Mark that we have the range and advance the option
			 * to what should
			 */

			/*
			 * be the range start. Check the value of the next
			 * parameter
			 */
			optionString = options[++x];
			if ((range = GetOptionValue(optionString)) < 0)
			{
				rc = OPT_RC_INVALID;
				printf("Error: Invalid range start identifier <%s>.\n",
				       optionString);
				break;
			}
			/* The default is to dump only one block */
			blockStart = blockEnd = (unsigned int) range;

			/*
			 * We have our range start marker, check if there is
			 * an end
			 */

			/*
			 * marker on the option line.  Assume that the last
			 * option
			 */

			/*
			 * is the file we are dumping, so check if there are
			 * options
			 */
			/* range start marker and the file */
			if (x <= (numOptions - 3))
			{
				if ((range = GetOptionValue(options[x + 1])) >= 0)
				{
					/* End range must be => start range */
					if (blockStart <= range)
					{
						blockEnd = (unsigned int) range;
						x++;
					}
					else
					{
						rc = OPT_RC_INVALID;
						printf("Error: Requested block range start <%d> is "
						       "greater than end <%d>.\n", blockStart, range);
						break;
					}
				}
			}
		}
		/*
		 * Check for the special case where the user forces a block
		 * size
		 */

		/*
		 * instead of having the tool determine it.  This is useful
		 * if
		 */

		/*
		 * the header of block 0 is corrupt and gives a garbage block
		 * size
		 */
		else if ((optionStringLength == 2)
			 && (strcmp(optionString, "-S") == 0))
		{
			int             localBlockSize;

			SET_OPTION(blockOptions, BLOCK_FORCED, 'S');
			/* Only accept the forced size option once */
			if (rc == OPT_RC_DUPLICATE)
				break;

			/*
			 * The token immediately following -S is the block
			 * size
			 */
			if (x >= (numOptions - 2))
			{
				rc = OPT_RC_INVALID;
				printf("Error: Missing block size identifier.\n");
				break;
			}
			/* Next option encountered must be forced block size */
			optionString = options[++x];
			if ((localBlockSize = GetOptionValue(optionString)) > 0)
				blockSize = (unsigned int) localBlockSize;
			else
			{
				rc = OPT_RC_INVALID;
				printf("Error: Invalid block size requested <%s>.\n",
				       optionString);
				break;
			}
		}
		/* The last option MUST be the file name */
		else if (x == (numOptions - 1))
		{

			/*
			 * Check to see if this looks like an option string
			 * before opening
			 */
			if (optionString[0] != '-')
			{
				fp = fopen(optionString, "rb");
				if (fp)
					fileName = options[x];
				else
				{
					rc = OPT_RC_FILE;
					printf("Error: Could not open file <%s>.\n", optionString);
					break;
				}
			}
			else
			{

				/*
				 * Could be the case where the help flag is
				 * used without a
				 */

				/*
				 * filename. Otherwise, the last option isn't
				 * a file
				 */
				if (strcmp(optionString, "-h") == 0)
					rc = OPT_RC_COPYRIGHT;
				else
				{
					rc = OPT_RC_FILE;
					printf("Error: Missing file name to dump.\n");
				}
				break;
			}
		}
		else
		{
			unsigned int    y;

			/*
			 * Option strings must start with '-' and contain
			 * switches
			 */
			if (optionString[0] != '-')
			{
				rc = OPT_RC_INVALID;
				printf("Error: Invalid option string <%s>.\n", optionString);
				break;
			}
			/*
			 * Iterate through the singular option string, throw
			 * out
			 */

			/*
			 * garbage, duplicates and set flags to be used in
			 * formatting
			 */
			for (y = 1; y < optionStringLength; y++)
			{
				switch (optionString[y])
				{
					/* Use absolute addressing               */
				case 'a':
					SET_OPTION(blockOptions, BLOCK_ABSOLUTE, 'a');
					break;

					/*
					 * Dump the binary contents of the
					 * page
					 */
				case 'b':
					SET_OPTION(blockOptions, BLOCK_BINARY, 'b');
					break;

					/*
					 * Dump the listed file as a control
					 * file
					 */
				case 'c':
					SET_OPTION(controlOptions, CONTROL_DUMP, 'c');
					break;

					/*
					 * Do not interpret the data. Format
					 * to hex and ascii.
					 */
				case 'd':
					SET_OPTION(blockOptions, BLOCK_NO_INTR, 'd');
					break;

					/*
					 * Format the contents of the block
					 * with interpretation
					 */
					/* of the headers */
				case 'f':
					SET_OPTION(blockOptions, BLOCK_FORMAT, 'f');
					break;

					/* Display the usage screen   */
				case 'h':
					rc = OPT_RC_COPYRIGHT;
					break;

					/* Format the items in detail */
				case 'i':
					SET_OPTION(itemOptions, ITEM_DETAIL, 'i');
					break;

					/* Interpret items as index values */
				case 'x':
					SET_OPTION(itemOptions, ITEM_INDEX, 'x');
					if (itemOptions & ITEM_HEAP)
					{
						rc = OPT_RC_INVALID;
						printf("Error: Options <y> and <x> are "
						   "mutually exclusive.\n");
					}
					break;

					/* Interpret items as heap values */
				case 'y':
					SET_OPTION(itemOptions, ITEM_HEAP, 'y');
					if (itemOptions & ITEM_INDEX)
					{
						rc = OPT_RC_INVALID;
						printf("Error: Options <x> and <y> are "
						   "mutually exclusive.\n");
					}
					break;
				case 'p':
					SET_OPTION(itemOptions, DEPARSE_HEAP, 'p');
					if (itemOptions & ITEM_INDEX)
					{
						rc = OPT_RC_INVALID;
						printf("Error: Options <p> and <x> are "
						   "mutually exclusive.\n");
					}
					break;
				case 'M':
					SET_OPTION(itemOptions, ITEM_CHECKSUM, 'M');
					break;

				default:
					rc = OPT_RC_INVALID;
					printf("Error: Unknown option <%c>.\n", optionString[y]);
					break;
				}

				if (rc)
					break;
			}
		}
	}

	if (rc == OPT_RC_DUPLICATE)
		printf("Error: Duplicate option listed <%c>.\n", duplicateSwitch);

	/* If the user requested a control file dump, a pure binary */
	/* block dump or a non-interpreted formatted dump, mask off */
	/* all other block level options (with a few exceptions)    */
	if (rc == OPT_RC_VALID)
	{
		/* The user has requested a control file dump, only -f and */
		/* -S are valid... turn off all other formatting */
		if (controlOptions & CONTROL_DUMP)
		{
			if ((blockOptions & ~(BLOCK_FORMAT | BLOCK_FORCED))
			    || (itemOptions))
			{
				rc = OPT_RC_INVALID;
				printf("Error: Invalid options used for Control File dump.\n"
				       "       Only options <Sf> may be used with <c>.\n");
			}
			else
			{
				controlOptions |=
					(blockOptions & (BLOCK_FORMAT | BLOCK_FORCED));
				blockOptions = itemOptions = 0;
			}
		}
		/* The user has request a binary block dump... only -R and */
		/* -f are honoured */
		else if (blockOptions & BLOCK_BINARY)
		{
			blockOptions &= (BLOCK_BINARY | BLOCK_RANGE | BLOCK_FORCED);
			itemOptions = 0;
		}
		/* The user has requested a non-intepreted dump... only -a, */
		/* -R and -f are honoured */
		else if (blockOptions & BLOCK_NO_INTR)
		{
			blockOptions &=
				(BLOCK_NO_INTR | BLOCK_ABSOLUTE | BLOCK_RANGE | BLOCK_FORCED);
			itemOptions = 0;
		}
	}
	return (rc);
}

/*
 * Build a tuple descriptor for the relation. This involves scanning
 * and building a valid Form_pg_attribute.
 */
static void
build_tupdesc(void)
{
	Form_pg_attribute *attrs;	/* XXX: make this dynamic */
	int             segno = 0;
	AttrNumber      maxattno = 0;

	attrs = malloc(sizeof(Form_pg_attribute) * 100);
	/* loop over each segno */
	do
	{
		char            segfile[MAXPGPATH];
		FILE           *seg;
		char            ext[10];

		if (segno > 0)
			sprintf(ext, ".%i", segno);
		else
			ext[0] = '\0';


		snprintf(segfile, sizeof(segfile), "%s/%u%s",
			 data_dir, AttributeRelationId, ext);
		seg = fopen(segfile, "r");

		if (!seg)
			break;

		while (!feof(seg))
		{
			char            buf[BLCKSZ];
			size_t          bytes;
			Page            pg;
			int             lines;
			OffsetNumber    lineoff;
			ItemId          lpp;

			if ((bytes = fread(buf, 1, BLCKSZ, seg)) != BLCKSZ)
			{
				if (bytes == 0)
					break;

				/* XXX: error */
			}
			pg = (Page) buf;
			lines = PageGetMaxOffsetNumber(pg);

			for (lineoff = FirstOffsetNumber, lpp = PageGetItemId(pg, lineoff);
			     lineoff <= lines;
			     lineoff++, lpp++)
			{
				if (ItemIdIsUsed(lpp))
				{
					HeapTupleHeader theader =
					(HeapTupleHeader) PageGetItem((Page) pg, lpp);
					HeapTuple       tuple = malloc(sizeof(HeapTupleData));
					Form_pg_attribute att;

					tuple->t_data = theader;
					att = (Form_pg_attribute) GETSTRUCT(tuple);
					if (att->attrelid == relid && att->attnum > 0)
					{
						Form_pg_attribute attcopy = malloc(ItemIdGetLength(lpp) - theader->t_hoff);
						memcpy(attcopy, att, ItemIdGetLength(lpp) - theader->t_hoff);
						attrs[att->attnum - 1] = attcopy;
						if (att->attnum > maxattno)
							maxattno = att->attnum;
					}
					else
					{
						free(tuple);
					}
				}
			}
		}
	} while (segno++ < 65536);

	tupdesc = malloc(sizeof(TupleDesc));
	tupdesc->natts = maxattno;
	tupdesc->attrs = attrs;
}



/* Given the index into the parameter list, convert and return the  */
/* current string to a number if possible  */
static int
GetOptionValue(char *optionString)
{
	unsigned int    x;
	int             value = -1;
	int             optionStringLength = strlen(optionString);

	/* Verify the next option looks like a number */
	for (x = 0; x < optionStringLength; x++)
		if (!isdigit((int) optionString[x]))
			break;

	/* Convert the string to a number if it looks good */
	if (x == optionStringLength)
		value = atoi(optionString);

	return (value);
}

/* Read the page header off of block 0 to determine the block size */
/* used in this file.  Can be overridden using the -S option.  The */
/* returned value is the block size of block 0 on disk */
static unsigned int
GetBlockSize()
{
	unsigned int    pageHeaderSize = sizeof(PageHeaderData);
	unsigned int    localSize = 0;
	int             bytesRead = 0;
	char            localCache[sizeof(PageHeaderData)];

	/* Read the first header off of block 0 to determine the block size */
	bytesRead = fread(&localCache, 1, pageHeaderSize, fp);
	rewind(fp);

	if (bytesRead == pageHeaderSize)
		localSize = (unsigned int) PageGetPageSize(&localCache);
	else
		printf("Error: Unable to read full page header from block 0.\n"
		       "  ===> Read %u bytes\n", bytesRead);
	return (localSize);
}

/* Determine the contents of the special section on the block and */
/* return this enum value */
static unsigned int
GetSpecialSectionType(Page page)
{
	unsigned int    rc = 0;
	unsigned int    specialOffset;
	unsigned int    specialSize;
	unsigned int    specialValue;
	PageHeader      pageHeader = (PageHeader) page;

	/* If this is not a partial header, check the validity of the  */
	/* special section offset and contents */
	if (bytesToFormat > sizeof(PageHeaderData))
	{
		specialOffset = (unsigned int) pageHeader->pd_special;

		/* Check that the special offset can remain on the block or */
		/* the partial block */
		if ((specialOffset == 0) ||
		    (specialOffset > blockSize) || (specialOffset > bytesToFormat))
			rc = SPEC_SECT_ERROR_BOUNDARY;
		else
		{
			specialSize = blockSize - specialOffset;

			/*
			 * If there is a special section, use its size to
			 * guess its
			 */
			/* contents */
			if (specialSize == 0)
				rc = SPEC_SECT_NONE;
			else if (specialSize == MAXALIGN(sizeof(uint32)))
			{

				/*
				 * If MAXALIGN is 8, this could be either a
				 * sequence or GIN
				 */
				if (bytesToFormat == blockSize)
				{
					specialValue = *((int *) (buffer + specialOffset));
					if (specialValue == SEQUENCE_MAGIC)
						rc = SPEC_SECT_SEQUENCE;
					else if (specialSize == MAXALIGN(sizeof(GinPageOpaqueData)))
						rc = SPEC_SECT_INDEX_GIN;
					else
						rc = SPEC_SECT_ERROR_UNKNOWN;
				}
				else
					rc = SPEC_SECT_ERROR_UNKNOWN;
			}
			else if (specialSize == MAXALIGN(sizeof(GinPageOpaqueData)))
				rc = SPEC_SECT_INDEX_GIN;
			else if (specialSize == MAXALIGN(sizeof(HashPageOpaqueData)))
			{

				/*
				 * As of 7.4, BTree and Hash pages have the
				 * same size special
				 */

				/*
				 * section.  Check for HASHO_FILL to detect
				 * if it's hash.
				 */

				/*
				 * As of 8.2, it could be GIST, too ... but
				 * there seems no
				 */

				/*
				 * good way to tell GIST from BTree :-(
				 * Also, HASHO_FILL is
				 */

				/*
				 * not reliable anymore, it could match
				 * cycleid by chance.
				 */

				/*
				 * Need to try to get some upstream changes
				 * to make this better.
				 */

				HashPageOpaque  hpo = (HashPageOpaque) (buffer + specialOffset);
				if (hpo->hasho_filler == HASHO_FILL)
					rc = SPEC_SECT_INDEX_HASH;
				else
					rc = SPEC_SECT_INDEX_BTREE;
			}
			else
				rc = SPEC_SECT_ERROR_UNKNOWN;
		}
	}
	else
		rc = SPEC_SECT_ERROR_UNKNOWN;

	return (rc);
}

/* Check whether page is a btree meta page */
static          bool
IsBtreeMetaPage(Page page)
{
	PageHeader      pageHeader = (PageHeader) page;

	if ((PageGetSpecialSize(page) == (MAXALIGN(sizeof(BTPageOpaqueData))))
	    && (bytesToFormat == blockSize))
	{
		/* As of 7.4, BTree and Hash pages have the same size special */
		/* section.  Check for HASHO_FILL to detect if it's hash. */
		BTPageOpaque    btpo =
		(BTPageOpaque) ((char *) page + pageHeader->pd_special);

		if ((((HashPageOpaque) (btpo))->hasho_filler != HASHO_FILL) &&
		    (btpo->btpo_flags & BTP_META))
			return true;
	}
	return false;
}

/* Display a header for the dump so we know the file name, the options */
/* used and the time the dump was taken */
static void
CreateDumpFileHeader(int numOptions, char **options)
{
	unsigned int    x;
	char            optionBuffer[52] = "\0";
	time_t          rightNow = time(NULL);

	/* Iterate through the options and cache them. */
	/* The maximum we can display is 50 option characters + spaces.   */
	for (x = 1; x < (numOptions - 1); x++)
	{
		if ((strlen(optionBuffer) + strlen(options[x])) > 50)
			break;
		strcat(optionBuffer, options[x]);
		strcat(optionBuffer, " ");
	}

	printf
		("\n*******************************************************************\n"
	  "* Greenplum File/Block Formatted Dump Utility - Version 3.1\n*\n"
		 "* File: %s\n"
		 "* Options used: %s\n*\n"
		 "* Dump created on: %s"
		 "*******************************************************************\n",
		 fileName, (strlen(optionBuffer)) ? optionBuffer : "None",
		 ctime(&rightNow));
}

/* Dump out a formatted block header for the requested block */
static int
FormatHeader(Page page)
{
	int             rc = 0;
	unsigned int    headerBytes;
	PageHeader      pageHeader = (PageHeader) page;

	printf("<Header> -----\n");

	/*
	 * Only attempt to format the header if the entire header (minus the
	 * item
	 */
	/* array) is available */
	if (bytesToFormat < offsetof(PageHeaderData, pd_linp[0]))
	{
		headerBytes = bytesToFormat;
		rc = EOF_ENCOUNTERED;
	}
	else
	{
		XLogRecPtr      pageLSN = PageGetLSN(page);
		int             maxOffset = PageGetMaxOffsetNumber(page);
		headerBytes = offsetof(PageHeaderData, pd_linp[0]);
		blockVersion = (unsigned int) PageGetPageLayoutVersion(page);

		/*
		 * The full header exists but we have to check that the item
		 * array
		 */
		/* is available or how far we can index into it */
		if (maxOffset > 0)
		{
			unsigned int    itemsLength = maxOffset * sizeof(ItemIdData);
			if (bytesToFormat < (headerBytes + itemsLength))
			{
				headerBytes = bytesToFormat;
				rc = EOF_ENCOUNTERED;
			}
			else
				headerBytes += itemsLength;
		}
		/* Interpret the content of the header */
		printf
			(" Block Offset: 0x%08x         Offsets: Lower    %4u (0x%04hx)\n"
			 " Block: Size %4d  Version %4u            Upper    %4u (0x%04hx)\n"
			 " LSN:  logid %6d recoff 0x%08x      Special  %4u (0x%04hx)\n"
			 " Items: %4d                   Free Space: %4u\n"
			 " Length (including item array): %u\n\n",
		     pageOffset, pageHeader->pd_lower, pageHeader->pd_lower,
			 (int) PageGetPageSize(page), blockVersion, pageHeader->pd_upper,
			 pageHeader->pd_upper,
		    pageLSN.xlogid, pageLSN.xrecoff, pageHeader->pd_special,
			 pageHeader->pd_special, maxOffset,
		  pageHeader->pd_upper - pageHeader->pd_lower, headerBytes);

		/*
		 * If it's a btree meta page, print the contents of the meta
		 * block.
		 */
		if (IsBtreeMetaPage(page))
		{
			BTMetaPageData *btpMeta = BTPageGetMeta(buffer);
			printf(" BTree Meta Data:  Magic (0x%08x)   Version (%u)\n"
			       "                   Root:     Block (%u)  Level (%u)\n"
			       "                   FastRoot: Block (%u)  Level (%u)\n\n",
			       btpMeta->btm_magic, btpMeta->btm_version,
			       btpMeta->btm_root, btpMeta->btm_level,
			     btpMeta->btm_fastroot, btpMeta->btm_fastlevel);
			headerBytes += sizeof(BTMetaPageData);
		}
		/*
		 * Eye the contents of the header and alert the user to
		 * possible
		 */
		/* problems. */
		if ((maxOffset < 0) ||
		    (maxOffset > blockSize) ||
		    (blockVersion != PG_PAGE_LAYOUT_VERSION) ||	/* only one we support */
		    (pageHeader->pd_upper > blockSize) ||
		    (pageHeader->pd_upper > pageHeader->pd_special) ||
		    (pageHeader->pd_lower <
		     (sizeof(PageHeaderData) - sizeof(ItemIdData)))
		    || (pageHeader->pd_lower > blockSize)
		    || (pageHeader->pd_upper < pageHeader->pd_lower)
		    || (pageHeader->pd_special > blockSize))
			printf(" Error: Invalid header information.\n\n");
	}

	/*
	 * If we have reached the end of file while interpreting the header,
	 * let
	 */
	/* the user know about it */
	if (rc == EOF_ENCOUNTERED)
		printf
			(" Error: End of block encountered within the header."
			 " Bytes read: %4u.\n\n", bytesToFormat);

	/* A request to dump the formatted binary of the block (header,  */
	/* items and special section).  It's best to dump even on an error */
	/* so the user can see the raw image. */
	if (blockOptions & BLOCK_FORMAT)
		FormatBinary(headerBytes, 0);

	return (rc);
}

/* fix hint bits for checksumming */
static void
fixupItemBlock(Page page)
{
	unsigned int    x;
	unsigned int    itemSize;
	unsigned int    itemOffset;
	unsigned int    itemFlags;
	ItemId          itemId;
	int             maxOffset = PageGetMaxOffsetNumber(page);

	/*
	 * If it's a btree meta page, the meta block is where items would
	 * normally be; don't print garbage. 
	 */
	if (IsBtreeMetaPage(page))
		return;

//	printf("<Data> ------ \n");

	/* Loop through the items on the block.  Check if the block is */
	/* empty and has a sensible item array listed before running  */
	/* through each item   */
	if (maxOffset == 0)
		printf(" Empty block - no items listed \n\n");
	else if ((maxOffset < 0) || (maxOffset > blockSize))
		printf(" Error: Item index corrupt on block. Offset: <%d>.\n\n",
		       maxOffset);
	else
	{
//		int             formatAs;
//		char            textFlags[8];

		for (x = 1; x < (maxOffset + 1); x++)
		{
			itemId = PageGetItemId(page, x);
			itemFlags = (unsigned int) ItemIdGetFlags(itemId);
			itemSize = (unsigned int) ItemIdGetLength(itemId);
			itemOffset = (unsigned int) ItemIdGetOffset(itemId);

//			printf(" Item %3u -- Length: %4u  Offset: %4u (0x%04x)"
//			"  Flags: %s\n", x, itemSize, itemOffset, itemOffset,
//			       textFlags);

			/*
			 * Make sure the item can physically fit on this
			 * block before
			 */
			/* formatting */
			if ((itemOffset + itemSize > blockSize) ||
			    (itemOffset + itemSize > bytesToFormat))
				printf("  Error: Item contents extend beyond block.\n"
				       "         BlockSize<%d> Bytes Read<%d> Item Start<%d>.\n",
				       blockSize, bytesToFormat, itemOffset + itemSize);
			else
			{
				unsigned int	numBytes	= itemSize;
				unsigned int	startIndex	= itemOffset;
				int             alignedSize = 
						MAXALIGN(sizeof(HeapTupleHeaderData));

				if (numBytes < alignedSize)
				{
					if (numBytes)
						printf("  Error: This item does not look like a heap item.\n");
				}
				else
				{
					HeapTupleHeader htup = 
							(HeapTupleHeader) (&buffer[startIndex]);

					htup->t_infomask = 0;
				}
			}
		}
	}
} // end fixupItemBlock

/* Dump out formatted items that reside on this block  */
static void
FormatItemBlock(Page page)
{
	unsigned int    x;
	unsigned int    itemSize;
	unsigned int    itemOffset;
	unsigned int    itemFlags;
	ItemId          itemId;
	int             maxOffset = PageGetMaxOffsetNumber(page);

	/*
	 * If it's a btree meta page, the meta block is where items would
	 * normally
	 */
	/* be; don't print garbage. */
	if (IsBtreeMetaPage(page))
		return;

	printf("<Data> ------ \n");

	/* Loop through the items on the block.  Check if the block is */
	/* empty and has a sensible item array listed before running  */
	/* through each item   */
	if (maxOffset == 0)
		printf(" Empty block - no items listed \n\n");
	else if ((maxOffset < 0) || (maxOffset > blockSize))
		printf(" Error: Item index corrupt on block. Offset: <%d>.\n\n",
		       maxOffset);
	else
	{
		int             formatAs;
		char            textFlags[8];

		/* First, honour requests to format items a special way, then  */
		/* use the special section to determine the format style */
		if (itemOptions & ITEM_INDEX)
			formatAs = ITEM_INDEX;
		else if (itemOptions & ITEM_HEAP)
			formatAs = ITEM_HEAP;
		else if (specialType != SPEC_SECT_NONE)
			formatAs = ITEM_INDEX;
		else
			formatAs = ITEM_HEAP;

		for (x = 1; x < (maxOffset + 1); x++)
		{
			itemId = PageGetItemId(page, x);
			itemFlags = (unsigned int) ItemIdGetFlags(itemId);
			itemSize = (unsigned int) ItemIdGetLength(itemId);
			itemOffset = (unsigned int) ItemIdGetOffset(itemId);

			if (itemFlags)
			{
/*				strcpy(textFlags, (LP_USED & itemFlags) ? "USED" : "DELETE"); 
 */
				if (ItemIdIsDead(itemId))
					strcpy(textFlags, "DEAD"); 
				else if (ItemIdDeleted(itemId))
					strcpy(textFlags, "DELETED"); 
				else
					strcpy(textFlags, "USED"); 
			}
			else
				sprintf(textFlags, "0x%02x", itemFlags);

			printf(" Item %3u -- Length: %4u  Offset: %4u (0x%04x)"
			"  Flags: %s\n", x, itemSize, itemOffset, itemOffset,
			       textFlags);

			/*
			 * Make sure the item can physically fit on this
			 * block before
			 */
			/* formatting */
			if ((itemOffset + itemSize > blockSize) ||
			    (itemOffset + itemSize > bytesToFormat))
				printf("  Error: Item contents extend beyond block.\n"
				       "         BlockSize<%d> Bytes Read<%d> Item Start<%d>.\n",
				       blockSize, bytesToFormat, itemOffset + itemSize);
			else
			{

				/*
				 * If the user requests that the items be
				 * interpreted as
				 */
				/* heap or index items...      */
				if (itemOptions & ITEM_DETAIL)
					FormatItem(itemSize, itemOffset, formatAs,
						   (itemOptions & DEPARSE_HEAP) == DEPARSE_HEAP);

				/* Dump the items contents in hex and ascii  */
				if (blockOptions & BLOCK_FORMAT)
					FormatBinary(itemSize, itemOffset);

				if (x == maxOffset)
					printf("\n");
			}
		}
	}
}

/* Interpret the contents of the item based on whether it has a special */
/* section and/or the user has hinted */
static void
FormatItem(unsigned int numBytes, unsigned int startIndex,
	   unsigned int formatAs, bool deparse)
{
	/* It is an index item, so dump the index header */
	if (formatAs == ITEM_INDEX)
	{
		if (numBytes < SizeOfIptrData)
		{
			if (numBytes)
				printf("  Error: This item does not look like an index item.\n");
		}
		else
		{
			IndexTuple      itup = (IndexTuple) (&(buffer[startIndex]));
			printf("  Block Id: %u  linp Index: %u  Size: %d\n"
			       "  Has Nulls: %u  Has Varwidths: %u\n\n",
			     ((uint32) ((itup->t_tid.ip_blkid.bi_hi << 16) |
				      (uint16) itup->t_tid.ip_blkid.bi_lo)),
			   itup->t_tid.ip_posid, (int) IndexTupleSize(itup),
			       IndexTupleHasNulls(itup), IndexTupleHasVarwidths(itup));

			if (numBytes != IndexTupleSize(itup))
				printf("  Error: Item size difference. Given <%u>, "
				       "Internal <%d>.\n", numBytes, (int) IndexTupleSize(itup));
		}
	}
	else
	{
		/* It is a heap item, so dump the heap header */
		int             alignedSize = MAXALIGN(sizeof(HeapTupleHeaderData));

		if (numBytes < alignedSize)
		{
			if (numBytes)
				printf("  Error: This item does not look like a heap item.\n");
		}
		else
		{
			char            flagString[256];
			unsigned int    x;
			unsigned int    bitmapLength = 0;
			unsigned int    oidLength = 0;
			unsigned int    computedLength;
			unsigned int    infoMask;
			int             localNatts;
			unsigned int    localHoff;
			bits8          *localBits;
			unsigned int    localBitOffset;

			HeapTupleHeader htup = (HeapTupleHeader) (&buffer[startIndex]);

			infoMask = htup->t_infomask;
			localBits = &(htup->t_bits[0]);
			localNatts = HeapTupleHeaderGetNatts(htup);
			localHoff = htup->t_hoff;
			localBitOffset = offsetof(HeapTupleHeaderData, t_bits);

			printf("  XMIN: %u  XMAX: %u  CMAX|XVAC: %u",
			       HeapTupleHeaderGetXmin(htup),
			       HeapTupleHeaderGetXmax(htup),
			       HeapTupleHeaderGetRawCommandId(htup));

			if (infoMask & HEAP_HASOID)
				printf("  OID: %u",
				       HeapTupleHeaderGetOid(htup));

			printf("\n"
			       "  Block Id: %u  linp Index: %u   Attributes: %d   Size: %d\n",
			       ((uint32)
				((htup->t_ctid.ip_blkid.bi_hi << 16) | (uint16) htup->
			     t_ctid.ip_blkid.bi_lo)), htup->t_ctid.ip_posid,
			       localNatts, htup->t_hoff);

			/*
			 * Place readable versions of the tuple info mask
			 * into a buffer.
			 */
			/* Assume that the string can not expand beyond 256. */
			flagString[0] = '\0';
			if (infoMask & HEAP_HASNULL)
				strcat(flagString, "HASNULL|");
			if (infoMask & HEAP_HASVARWIDTH)
				strcat(flagString, "HASVARWIDTH|");
			if (infoMask & HEAP_HASEXTERNAL)
				strcat(flagString, "HASEXTERNAL|");
			if (infoMask & HEAP_HASCOMPRESSED)
				strcat(flagString, "HASCOMPRESSED|");
			if (infoMask & HEAP_HASOID)
				strcat(flagString, "HASOID|");
			if (infoMask & HEAP_XMAX_EXCL_LOCK)
				strcat(flagString, "XMAX_EXCL_LOCK|");
			if (infoMask & HEAP_XMAX_SHARED_LOCK)
				strcat(flagString, "XMAX_SHARED_LOCK|");
			if (infoMask & HEAP_XMIN_COMMITTED)
				strcat(flagString, "XMIN_COMMITTED|");
			if (infoMask & HEAP_XMIN_INVALID)
				strcat(flagString, "XMIN_INVALID|");
			if (infoMask & HEAP_XMAX_COMMITTED)
				strcat(flagString, "XMAX_COMMITTED|");
			if (infoMask & HEAP_XMAX_INVALID)
				strcat(flagString, "XMAX_INVALID|");
			if (infoMask & HEAP_XMAX_IS_MULTI)
				strcat(flagString, "XMAX_IS_MULTI|");
			if (infoMask & HEAP_UPDATED)
				strcat(flagString, "UPDATED|");
			if (infoMask & HEAP_MOVED_OFF)
				strcat(flagString, "MOVED_OFF|");
			if (infoMask & HEAP_MOVED_IN)
				strcat(flagString, "MOVED_IN|");
			if (strlen(flagString))
				flagString[strlen(flagString) - 1] = '\0';

			printf("  infomask: 0x%04x (%s) \n", infoMask, flagString);

			if (deparse)
			{
				HeapTuple       tuple = malloc(sizeof(HeapTupleData));
				AttrNumber      attno;
				char            deparsed[8192];

				tuple->t_data = htup;
				deparsed[0] = '\0';

				for (attno = 1; attno <= localNatts; attno++)
				{
					Form_pg_attribute att = tupdesc->attrs[attno - 1];
					bool            isnull;
					Datum           datum = heap_getattr(tuple, attno, tupdesc, &isnull);

					if (isnull)
						strcat(deparsed, "<NULL>");
					else
					{
						switch (att->atttypid)
						{
						case TEXTOID:
							{

								/*
								 * XXX: can't
								 * do toast
								 */
								struct varlena *va =
								(struct varlena *) DatumGetPointer(datum);
								struct varlena      *attrib = (struct varlena *) va;

								if (VARATT_IS_EXTENDED(attrib))
								{
									if (VARATT_IS_SHORT(attrib))
									{
										int             len = strlen(deparsed);
										int             tsize = VARSIZE_SHORT(va) - VARHDRSZ_SHORT;
										memcpy(deparsed + len, VARDATA_SHORT(va),
										       tsize);
										deparsed[len + tsize] = '\0';
									}
									else
										strcat(deparsed,
										       "cannot deparse TOASTed attribute");
								}
								else
								{
									int             len = strlen(deparsed);

									memcpy(deparsed + len, VARDATA(va),
									       VARSIZE(va) - VARHDRSZ);
									deparsed[len + VARSIZE(va) - VARHDRSZ] = '\0';
								}
							}
							break;
						case NUMERICOID:
							{
								struct varlena      *attrib = DatumGetPointer(datum);
								char           *str;

								if (VARATT_IS_EXTENDED(attrib))
								{
									if (VARATT_IS_SHORT(attrib))
									{
										unsigned        size = VARSIZE_SHORT(attrib);
										unsigned        new_size = size - VARHDRSZ_SHORT + VARHDRSZ;
										struct varlena      *tmp = attrib;

										attrib = (struct varlena *) malloc(new_size);
										SET_VARSIZE(attrib, new_size);
										memcpy(VARDATA(attrib), VARDATA_SHORT(tmp), size - VARHDRSZ_SHORT);
									}
									else
										strcat(deparsed,
										       "cannot deparse TOASTed attribute");
								}
								str = num_out((Numeric) attrib);
								strcat(deparsed, str);
							}
							break;
						case BOOLOID:
							{
								int             len = strlen(deparsed);

								if (DatumGetBool(datum))
									deparsed[len] = 't';
								else
									deparsed[len] = 'f';
								deparsed[len + 1] = '\0';
							}
							break;
						case CHAROID:
							{
								char            buf[2];
								char            ch = DatumGetChar(datum);
								buf[0] = ch;
								buf[1] = '\0';
								strcat(deparsed, buf);
							}
							break;
						case FLOAT8OID:
							{
								char            buf[128 + 1];
								float4          num = DatumGetFloat8(datum);

								if (isnan(num))
								{
									strcat(deparsed, "NaN");
									break;
								}
								switch (isinf(num))
								{
								case 1:
									strcpy(buf, "Infinity");
									break;
								default:
									{
										int             ndig = DBL_DIG;

										if (ndig < 1)
											ndig = 1;

										sprintf(buf, "%.*g", ndig, num);
									}
								}
								strcat(deparsed, buf);
							}
							break;

						case FLOAT4OID:
							{
								char            buf[64 + 1];
								float4          num = DatumGetFloat4(datum);

								if (isnan(num))
								{
									strcat(deparsed, "NaN");
									break;
								}
								switch (isinf(num))
								{
								case 1:
									strcpy(buf, "Infinity");
									break;
								default:
									{
										int             ndig = FLT_DIG;

										if (ndig < 1)
											ndig = 1;

										sprintf(buf, "%.*g", ndig, num);
									}
								}
								strcat(deparsed, buf);
							}
							break;
						case NAMEOID:
							{
								char           *tmp = NameStr(*DatumGetName(datum));
								strcat(deparsed, tmp);
							}
							break;
						case INT4OID:
							{
								char           *tmp = malloc(12);
								pg_ltoa(DatumGetInt32(datum), tmp);
								strcat(deparsed, tmp);
							}
							break;
						case INT8OID:
							{
								int64           val = DatumGetInt64(datum);
								char            buf[26];

								if (snprintf(buf, sizeof(buf), INT64_FORMAT, val) < 0)
									elog(ERROR, "could not format int8");

								strcat(deparsed, buf);

							}
						case INT2OID:
							{
								char            buf[7];

								pg_itoa(DatumGetInt16(datum), buf);

								strcat(deparsed, buf);
							}
							break;
						case OIDOID:
						case XIDOID:
							{

								char            buf[12];

								snprintf(buf, sizeof(buf), "%u",
									 att->atttypid == OIDOID ? (Oid) datum :
									 (TransactionId) datum);
								strcat(deparsed, buf);
							}
							break;
						case 1034:
							strcat(deparsed, "<cannot deparse _aclitem>");
							break;
						default:
							{
								char            buf[256];

								sprintf(buf, "<unknown type %u>", att->atttypid);
								strcat(deparsed, buf);
							}
							break;
						}
					}
					strcat(deparsed, "|");
				}
				//if (deparsed)
					deparsed[strlen(deparsed) - 1] = '\0';
				printf("Deparsed data: \"%s\"\n", deparsed);
			}
			/*
			 * As t_bits is a variable length array, determine
			 * the length of
			 */
			/* the header proper   */
			if (infoMask & HEAP_HASNULL)
				bitmapLength = BITMAPLEN(localNatts);
			else
				bitmapLength = 0;

			if (infoMask & HEAP_HASOID)
				oidLength += sizeof(Oid);

			computedLength =
				MAXALIGN(localBitOffset + bitmapLength + oidLength);

			/*
			 * Inform the user of a header size mismatch or dump
			 * the t_bits array
			 */
			if (computedLength != localHoff)
				printf
					("  Error: Computed header length not equal to header size.\n"
					 "         Computed <%u>  Header: <%d>\n", computedLength,
					 localHoff);
			else if ((infoMask & HEAP_HASNULL) && bitmapLength)
			{
				printf("  t_bits: ");
				for (x = 0; x < bitmapLength; x++)
				{
					printf("[%u]: 0x%02x ", x, localBits[x]);
					if (((x & 0x03) == 0x03) && (x < bitmapLength - 1))
						printf("\n          ");
				}
				printf("\n");
			}
			printf("\n");
		}
	}
}


/* On blocks that have special sections, we have to interpret the */
/* contents based on size of the special section (since there is */
/* no other way) */
static void
FormatSpecial()
{
	PageHeader      pageHeader = (PageHeader) buffer;
	char            flagString[100] = "\0";
	unsigned int    specialOffset = pageHeader->pd_special;
	unsigned int    specialSize =
	(blockSize >= specialOffset) ? (blockSize - specialOffset) : 0;

	printf("<Special Section> -----\n");

	switch (specialType)
	{
	case SPEC_SECT_ERROR_UNKNOWN:
	case SPEC_SECT_ERROR_BOUNDARY:
		printf(" Error: Invalid special section encountered.\n");
		break;

	case SPEC_SECT_SEQUENCE:
		printf(" Sequence: 0x%08x\n", SEQUENCE_MAGIC);
		break;

		/* Btree index section   */
	case SPEC_SECT_INDEX_BTREE:
		{
			BTPageOpaque    btreeSection = (BTPageOpaque) (buffer + specialOffset);
			if (btreeSection->btpo_flags & BTP_LEAF)
				strcat(flagString, "LEAF|");
			if (btreeSection->btpo_flags & BTP_ROOT)
				strcat(flagString, "ROOT|");
			if (btreeSection->btpo_flags & BTP_DELETED)
				strcat(flagString, "DELETED|");
			if (btreeSection->btpo_flags & BTP_META)
				strcat(flagString, "META|");
			if (btreeSection->btpo_flags & BTP_HALF_DEAD)
				strcat(flagString, "HALFDEAD|");
			if (btreeSection->btpo_flags & BTP_SPLIT_END)
				strcat(flagString, "SPLITEND|");
			if (btreeSection->btpo_flags & BTP_HAS_GARBAGE)
				strcat(flagString, "HASGARBAGE|");
			if (strlen(flagString))
				flagString[strlen(flagString) - 1] = '\0';

			printf(" BTree Index Section:\n"
			       "  Flags: 0x%04x (%s)\n"
			       "  Blocks: Previous (%d)  Next (%d)  %s (%d)  CycleId (%d)\n\n",
			       btreeSection->btpo_flags, flagString,
			   btreeSection->btpo_prev, btreeSection->btpo_next,
			       (btreeSection->
			   btpo_flags & BTP_DELETED) ? "Next XID" : "Level",
			       btreeSection->btpo.level,
			       btreeSection->btpo_cycleid);
		}
		break;

		/* Hash index section   */
	case SPEC_SECT_INDEX_HASH:
		{
			HashPageOpaque  hashSection = (HashPageOpaque) (buffer + specialOffset);
			if (hashSection->hasho_flag & LH_UNUSED_PAGE)
				strcat(flagString, "UNUSED|");
			if (hashSection->hasho_flag & LH_OVERFLOW_PAGE)
				strcat(flagString, "OVERFLOW|");
			if (hashSection->hasho_flag & LH_BUCKET_PAGE)
				strcat(flagString, "BUCKET|");
			if (hashSection->hasho_flag & LH_BITMAP_PAGE)
				strcat(flagString, "BITMAP|");
			if (hashSection->hasho_flag & LH_META_PAGE)
				strcat(flagString, "META|");
			if (strlen(flagString))
				flagString[strlen(flagString) - 1] = '\0';
			printf(" Hash Index Section:\n"
			       "  Flags: 0x%04x (%s)\n"
			       "  Bucket Number: 0x%04x\n"
			       "  Blocks: Previous (%d)  Next (%d)\n\n",
			       hashSection->hasho_flag, flagString,
			       hashSection->hasho_bucket,
			       hashSection->hasho_prevblkno, hashSection->hasho_nextblkno);
		}
		break;

		/* GIST index section */
	case SPEC_SECT_INDEX_GIST:
		{
			GISTPageOpaque  gistSection = (GISTPageOpaque) (buffer + specialOffset);
			if (gistSection->flags & F_LEAF)
				strcat(flagString, "LEAF|");
			if (gistSection->flags & F_DELETED)
				strcat(flagString, "DELETED|");
			if (gistSection->flags & F_TUPLES_DELETED)
				strcat(flagString, "TUPLESDELETED|");
			if (strlen(flagString))
				flagString[strlen(flagString) - 1] = '\0';
			printf(" GIST Index Section:\n"
			       "  Flags: 0x%08x (%s)\n"
			       "  Blocks: RightLink (%d)\n\n",
			       gistSection->flags, flagString,
			       gistSection->rightlink);
		}
		break;

		/* GIN index section */
	case SPEC_SECT_INDEX_GIN:
		{
			GinPageOpaque   ginSection = (GinPageOpaque) (buffer + specialOffset);
			if (ginSection->flags & GIN_DATA)
				strcat(flagString, "DATA|");
			if (ginSection->flags & GIN_LEAF)
				strcat(flagString, "LEAF|");
			if (ginSection->flags & GIN_DELETED)
				strcat(flagString, "DELETED|");
			if (strlen(flagString))
				flagString[strlen(flagString) - 1] = '\0';
			printf(" GIN Index Section:\n"
			       "  Flags: 0x%08x (%s)  Maxoff: %d\n"
			       "  Blocks: RightLink (%d)\n\n",
			       ginSection->flags, flagString,
			       ginSection->maxoff,
			       ginSection->rightlink);
		}
		break;

		/* No idea what type of special section this is */
	default:
		printf(" Unknown special section type. Type: <%u>.\n", specialType);
		break;
	}

	/* Dump the formatted contents of the special section        */
	if (blockOptions & BLOCK_FORMAT)
	{
		if (specialType == SPEC_SECT_ERROR_BOUNDARY)
			printf(" Error: Special section points off page."
			       " Unable to dump contents.\n");
		else
			FormatBinary(specialSize, specialOffset);
	}
}

/* Dump the binary image of the block */
static void
DumpBinaryBlock()
{
	unsigned int    x;
	for (x = 0; x < bytesToFormat; x++)
		putchar(buffer[x]);
}

/* For each block, dump out formatted header and content information */
static void
FormatBlock()
{
	Page            page = (Page) buffer;
	pageOffset = blockSize * currentBlock;
	specialType = GetSpecialSectionType(page);

	if (itemOptions & ITEM_CHECKSUM)
	{
		if (bytesToFormat >= offsetof(PageHeaderData, pd_linp[0]))
		{
			fixupItemBlock(page);
//			FormatBinary(bytesToFormat, 0);			
			DumpBinaryBlock();
		}
		else
		{
			int rc;
			rc = FormatHeader(page);
		}
			
		return;
	}

	printf("\nBlock %4u **%s***************************************\n",
	       currentBlock,
	       (bytesToFormat ==
		blockSize) ? "***************" : " PARTIAL BLOCK ");

	/* Either dump out the entire block in hex+acsii fashion or */
	/* interpret the data based on block structure  */
	if (blockOptions & BLOCK_NO_INTR)
		FormatBinary(bytesToFormat, 0);
	else
	{
		int             rc;

		/*
		 * Every block contains a header, items and possibly a
		 * special
		 */
		/* section.  Beware of partial block reads though             */
		rc = FormatHeader(page);

		/*
		 * If we didn't encounter a partial read in the header, carry
		 * on...
		 */
		if (rc != EOF_ENCOUNTERED)
		{
			FormatItemBlock(page);

			if (specialType != SPEC_SECT_NONE)
				FormatSpecial();
		}
	}
}

/* Dump out the content of the PG control file */
static void
FormatControl()
{
	unsigned int    localPgVersion = 0;
	unsigned int    controlFileSize = 0;

	printf
		("\n<pg_control Contents> *********************************************\n\n");

	/* Check the version  */
	if (bytesToFormat >= offsetof(ControlFileData, catalog_version_no))
		localPgVersion = ((ControlFileData *) buffer)->pg_control_version;

	if (localPgVersion >= 72)
		controlFileSize = sizeof(ControlFileData);
	else
	{
		printf("gp_filedump: PostgreSQL %u not supported.\n", localPgVersion);
		return;
	}

	/* Interpret the control file if it's all there */
	if (bytesToFormat >= controlFileSize)
	{
		ControlFileData *controlData = (ControlFileData *) buffer;
		CheckPoint     *checkPoint = &(controlData->checkPointCopy);
		pg_crc32        crcLocal;
		pg_crc32        crcLocal_old;
		char           *dbState;

		/* Compute a local copy of the CRC to verify the one on disk */
		crcLocal = crc32c(crc32cInit(), buffer, offsetof(ControlFileData, crc));
		crc32cFinish(crcLocal);
		/* Also compute a local copy of the CRC using old algorithm to verify the one on disk */
		INIT_CRC32(crcLocal_old);
		COMP_CRC32(crcLocal_old, buffer, offsetof(ControlFileData, crc));
		FIN_CRC32(crcLocal_old);

		/* Grab a readable version of the database state */
		switch (controlData->state)
		{
		case DB_STARTUP:
			dbState = "STARTUP";
			break;
		case DB_SHUTDOWNED:
			dbState = "SHUTDOWNED";
			break;
		case DB_SHUTDOWNING:
			dbState = "SHUTDOWNING";
			break;
		case DB_IN_CRASH_RECOVERY:
			dbState = "IN CRASH RECOVERY";
			break;
		case DB_IN_ARCHIVE_RECOVERY:
			dbState = "IN ARCHIVE RECOVERY";
			break;
		case DB_IN_PRODUCTION:
			dbState = "IN PRODUCTION";
			break;
		default:
			dbState = "UNKNOWN";
			break;
		}

		printf("                          CRC: %s\n"
		       "           pg_control Version: %u%s\n"
		       "              Catalog Version: %u\n"
		       "            System Identifier: " UINT64_FORMAT "\n"
		       "                        State: %s\n"
		       "              Last Checkpoint: %s"
		       "             Current Log File: %u\n"
		       "             Next Log Segment: %u\n"
		       "       Last Checkpoint Record: Log File (%u) Offset (0x%08x)\n"
		       "   Previous Checkpoint Record: Log File (%u) Offset (0x%08x)\n"
		       "  Last Checkpoint Record Redo: Log File (%u) Offset (0x%08x)\n"
		       "             |-          Undo: Log File (%u) Offset (0x%08x)\n"
		       "             |-    TimeLineID: %u\n"
		       "             |-      Next XID: %u\n"
		       "             |-      Next OID: %u\n"
		       "             |-    Next Multi: %u\n"
		       "             |- Next MultiOff: %u\n"
		       "             |-          Time: %s"
		       "       Maximum Data Alignment: %u\n"
		       "        Floating-Point Sample: %.7g%s\n"
		       "          Database Block Size: %u\n"
		       "           Blocks Per Segment: %u\n"
		       "            XLOG Segment Size: %u\n"
		       "    Maximum Identifier Length: %u\n"
		       "           Maximum Index Keys: %u\n"
		       "   Date and Time Type Storage: %s\n"
		       "         Locale Buffer Length: %u\n"
		       "                   lc_collate: %s\n"
		       "                     lc_ctype: %s\n\n",
		       EQ_CRC32(crcLocal, controlData->crc) ? "Correct (crc32c)" :
		    		   EQ_CRC32(crcLocal_old, controlData->crc) ? "Correct (classic crc32)" : "Not Correct",
		       controlData->pg_control_version,
		    (controlData->pg_control_version == PG_CONTROL_VERSION ?
		     "" : " (Not Correct!)"),
		       controlData->catalog_version_no,
		       controlData->system_identifier,
		       dbState,
		       ctime(&(controlData->time)), controlData->logId,
		       controlData->logSeg, controlData->checkPoint.xlogid,
		       controlData->checkPoint.xrecoff,
		       controlData->prevCheckPoint.xlogid,
		controlData->prevCheckPoint.xrecoff, checkPoint->redo.xlogid,
		       checkPoint->redo.xrecoff, checkPoint->undo.xlogid,
		       checkPoint->undo.xrecoff, checkPoint->ThisTimeLineID,
		       checkPoint->nextXid, checkPoint->nextOid,
		       checkPoint->nextMulti, checkPoint->nextMultiOffset,
		       ctime(&checkPoint->time),
		       controlData->maxAlign,
		       controlData->floatFormat,
		       (controlData->floatFormat == FLOATFORMAT_VALUE ?
			"" : " (Not Correct!)"),
		       controlData->blcksz,
		       controlData->relseg_size,
		       controlData->xlog_seg_size,
		       controlData->nameDataLen,
		       controlData->indexMaxKeys,
		       (controlData->enableIntTimes ?
			"64 bit Integers" : "Floating Point"),
		       controlData->localeBuflen, controlData->lc_collate,
		       controlData->lc_ctype);
	}
	else
	{
		printf(" Error: pg_control file size incorrect.\n"
		       "        Size: Correct <%u>  Received <%u>.\n\n",
		       controlFileSize, bytesToFormat);

		/* If we have an error, force a formatted dump so we can see  */
		/* where things are going wrong */
		controlOptions |= CONTROL_FORMAT;
	}

	/* Dump hex and ascii representation of data  */
	if (controlOptions & CONTROL_FORMAT)
	{
		printf("<pg_control Formatted Dump> *****************"
		       "**********************\n\n");
		FormatBinary(bytesToFormat, 0);
	}
}

/* Dump out the contents of the block in hex and ascii.  */
/* BYTES_PER_LINE bytes are formatted in each line. */
static void
FormatBinary(unsigned int numBytes, unsigned int startIndex)
{
	unsigned int    index = 0;
	unsigned int    stopIndex = 0;
	unsigned int    x = 0;
	unsigned int    lastByte = startIndex + numBytes;

	if (numBytes)
	{

		/*
		 * Iterate through a printable row detailing the current
		 * address, the hex and ascii values
		 */
		for (index = startIndex; index < lastByte; index += BYTES_PER_LINE)
		{
			stopIndex = index + BYTES_PER_LINE;

			/* Print out the address */
			if (blockOptions & BLOCK_ABSOLUTE)
				printf("  %08x: ", (unsigned int) (pageOffset + index));
			else
				printf("  %04x: ", (unsigned int) index);

			/* Print out the hex version of the data */
			for (x = index; x < stopIndex; x++)
			{
				if (x < lastByte)
					printf("%02x", 0xff & ((unsigned) buffer[x]));
				else
					printf("  ");
				if ((x & 0x03) == 0x03)
					printf(" ");
			}
			printf(" ");

			/* Print out the ascii version of the data */
			for (x = index; x < stopIndex; x++)
			{
				if (x < lastByte)
					printf("%c", isprint(buffer[x]) ? buffer[x] : '.');
				else
					printf(" ");
			}
			printf("\n");
		}
		printf("\n");
	}
}

/* Control the dumping of the blocks within the file */
static void
DumpFileContents()
{
	unsigned int    initialRead = 1;
	unsigned int    contentsToDump = 1;

	/* If the user requested a block range, seek to the correct position */
	/* within the file for the start block. */
	if (blockOptions & BLOCK_RANGE)
	{
		unsigned int    position = blockSize * blockStart;
		if (fseek(fp, position, SEEK_SET) != 0)
		{
			printf("Error: Seek error encountered before requested "
			       "start block <%d>.\n", blockStart);
			contentsToDump = 0;
		}
		else
			currentBlock = blockStart;
	}
	/* Iterate through the blocks in the file until you reach the end or */
	/* the requested range end */
	while (contentsToDump)
	{
		bytesToFormat = fread(buffer, 1, blockSize, fp);

		if (bytesToFormat == 0)
		{

			/*
			 * fseek() won't pop an error if you seek passed eof.
			 * The next
			 */
			/* subsequent read gets the error.     */
			if (initialRead)
				printf("Error: Premature end of file encountered.\n");
			else if (!(blockOptions & BLOCK_BINARY))
				printf("\n*** End of File Encountered. Last Block "
				       "Read: %d ***\n", currentBlock - 1);

			contentsToDump = 0;
		}
		else
		{
			if (blockOptions & BLOCK_BINARY)
				DumpBinaryBlock();
			else
			{
				if (controlOptions & CONTROL_DUMP)
				{
					FormatControl();
					contentsToDump = false;
				}
				else
					FormatBlock();
			}
		}

		/* Check to see if we are at the end of the requested range. */
		if ((blockOptions & BLOCK_RANGE) &&
		    (currentBlock >= blockEnd) && (contentsToDump))
		{

			/*
			 * Don't print out message if we're doing a binary
			 * dump
			 */
			if (!(blockOptions & BLOCK_BINARY))
				printf("\n*** End of Requested Range Encountered. "
				 "Last Block Read: %d ***\n", currentBlock);
			contentsToDump = 0;
		}
		else
			currentBlock++;

		initialRead = 0;
	}
}

/*
 * Consume the options and iterate through the given file, formatting as
 * requested.
 */
int
main(int argv, char **argc)
{
	/* If there is a parameter list, validate the options  */
	unsigned int    validOptions;
	validOptions = (argv < 2) ? OPT_RC_COPYRIGHT : ConsumeOptions(argv, argc);

	/*
	 * Display valid options if no parameters are received or invalid
	 * options where encountered
	 */
	if (validOptions != OPT_RC_VALID)
		DisplayOptions(validOptions);
	else
	{
		if (itemOptions & DEPARSE_HEAP)
		{
			file_set_dirs(fileName);
			build_tupdesc();
		}
		/* Don't dump the header if we're dumping binary pages         */
		if ((!(blockOptions & BLOCK_BINARY))
			&& (!(itemOptions & ITEM_CHECKSUM)))
			CreateDumpFileHeader(argv, argc);

		/*
		 * If the user has not forced a block size, use the size of
		 * the control file data or the information from the block 0
		 * header
		 */
		if (controlOptions)
		{
			if (!(controlOptions & CONTROL_FORCED))
				blockSize = sizeof(ControlFileData);
		}
		else if (!(blockOptions & BLOCK_FORCED))
			blockSize = GetBlockSize();

		/* On a positive block size, allocate a local buffer to store */
		/* the subsequent blocks */
		if (blockSize > 0)
		{
			buffer = (char *) malloc(blockSize);
			if (buffer)
				DumpFileContents();
			else
				printf("\nError: Unable to create buffer of size <%d>.\n",
				       blockSize);
		}
	}

	/* Close out the file and get rid of the allocated block buffer */
	if (fp)
		fclose(fp);

	if (buffer)
		free(buffer);

	exit(0);
}

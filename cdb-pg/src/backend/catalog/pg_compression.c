/*
 * Copyright (c) 2011 EMC Corporation All Rights Reserved
 *
 * This software is protected, without limitation, by copyright law
 * and international treaties. Use of this software and the intellectual
 * property contained therein is expressly limited to the terms and
 * conditions of the License Agreement under which it is provided by
 * or on behalf of EMC.
 *
 * ---------------------------------------------------------------------
 *
 * Interfaces to low level compression functionality.
 */

#include "postgres.h"
#include "fmgr.h"

#include "access/genam.h"
#include "access/catquery.h"
#include "access/reloptions.h"
#include "access/tupdesc.h"
#include "access/tupmacs.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_compression.h"
#include "catalog/dependency.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbappendonlystoragelayer.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "storage/gp_compress.h"
#include "storage/quicklz1.h"
#include "storage/quicklz3.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

/* names we expect to see in ENCODING clauses */
char *storage_directive_names[] = {"compresstype", "compresslevel",
								   "blocksize", NULL};


/* Internal state for quicklz */
typedef struct quicklz_state
{
	void *scratch;

	int level;
	bool compress;

	/*
	 * The actual algorithms. Allows us to handle quicklz1 and quicklz3
	 * conveniently.
	 */
	size_t (*compress_fn)(int level, const void *, char *, size_t, void *);
	size_t (*decompress_fn)(int level, const char *, void *, void *);
} quicklz_state;

/* Internal state for zlib */
typedef struct zlib_state
{
	int level;			/* compression level */
	bool compress;		/* compress or decompress? */

	/*
	 * The compression and decompression functions.
	 */
	int (*compress_fn) (Bytef *dest,
						uLongf *destLen,
						const Bytef *source,
						uLong sourceLen,
						int level);  

	int (*decompress_fn) (Bytef *dest,
						  uLongf *destLen,
						  const Bytef *source,
						  uLong sourceLen);

} zlib_state;

static NameData
comptype_to_name(char *comptype)
{
	char		   *dct; /* down cased comptype */
	size_t			len;
	NameData		compname;


	if (strlen(comptype) >= NAMEDATALEN)
		elog(ERROR, "compression name \"%s\" exceeds maximum name length "
			 "of %d bytes", comptype, NAMEDATALEN - 1);

	len = strlen(comptype);
	dct = str_tolower(comptype, len);
	len = strlen(dct);
	
	memcpy(&(NameStr(compname)), dct, len);
	NameStr(compname)[len] = '\0';

	return compname;
}

/* 
 * Find the compression implementation (in pg_compression) for a particular
 * compression type.
 *
 * Comparison is case insensitive.
 */
PGFunction *
GetCompressionImplementation(char *comptype)
{
	HeapTuple		tuple;
	NameData		compname;
	PGFunction	   *funcs;
	Form_pg_compression ctup;
	FmgrInfo		finfo;

	compname = comptype_to_name(comptype);

	tuple = caql_getfirst(
			NULL,
			cql("SELECT * FROM pg_compression "
				" WHERE compname = :1 ",
				NameGetDatum(&compname)));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("unknown compress type \"%s\"",
						comptype)));

	funcs = palloc0(sizeof(PGFunction) * NUM_COMPRESS_FUNCS);
	
	ctup = (Form_pg_compression)GETSTRUCT(tuple);

	Insist(OidIsValid(ctup->compconstructor));
	fmgr_info(ctup->compconstructor, &finfo);
	funcs[COMPRESSION_CONSTRUCTOR] = finfo.fn_addr;

	Insist(OidIsValid(ctup->compdestructor));
	fmgr_info(ctup->compdestructor, &finfo);
	funcs[COMPRESSION_DESTRUCTOR] = finfo.fn_addr;

	Insist(OidIsValid(ctup->compcompressor));
	fmgr_info(ctup->compcompressor, &finfo);
	funcs[COMPRESSION_COMPRESS] = finfo.fn_addr;

	Insist(OidIsValid(ctup->compdecompressor));
	fmgr_info(ctup->compdecompressor, &finfo);
	funcs[COMPRESSION_DECOMPRESS] = finfo.fn_addr;

	Insist(OidIsValid(ctup->compvalidator));
	fmgr_info(ctup->compvalidator, &finfo);
	funcs[COMPRESSION_VALIDATOR] = finfo.fn_addr;

	return funcs;
}

/* Invokes a compression constructor */
CompressionState *
callCompressionConstructor(PGFunction constructor,
						   TupleDesc tupledesc,
						   StorageAttributes *sa,
						   bool is_compress)
{
  return DatumGetPointer(DirectFunctionCall3(constructor,
											 PointerGetDatum(tupledesc),
											 PointerGetDatum(sa),
											 BoolGetDatum(is_compress)));

}

void
callCompressionDestructor(PGFunction destructor, CompressionState *state)
{
	DirectFunctionCall1(destructor, PointerGetDatum(state));
}

/* Actually call a compression (or decompression) function */
void
callCompressionActuator(PGFunction func , const void *src , int32 src_sz,
						char *dst, int32 dst_sz, int32 *dst_used,
						CompressionState *state)
{

  (void)DirectFunctionCall6(func, PointerGetDatum(src), Int32GetDatum(src_sz),
							PointerGetDatum(dst), Int32GetDatum(dst_sz),
							PointerGetDatum(dst_used), PointerGetDatum(state));


}

void
callCompressionValidator(PGFunction func, char *comptype, int32 complevel,
						 int32 blocksize, Oid typid)
{
	StorageAttributes sa;

	sa.comptype = comptype;
	sa.complevel = complevel;
	sa.blocksize = blocksize;
	sa.typid = typid;
	(void)DirectFunctionCall1(func, PointerGetDatum(&sa));
}

/*
 * quicklz helper function.
 */
static size_t
quicklz_desired_sz(size_t input)
{
	/*
	 * From the QuickLZ manual:
	 *
	 *   "The destination buffer must be at least size + 400 bytes large because
	 *   incompressible data may increase in size."
	 *
	 */
	return input + 400;
}

/*
 * Wrap up the qlz functions since C's type checking is getting in the
 * way.
 */
static size_t
quicklz_compressor(int level, const void *source, char *destination,
				   size_t size, void *state)
{
	if (level == 1)
	{
		return qlz1_compress(source, destination, size,
							 (qlz1_state_compress *)state);
	}
	else if (level == 3)
	{
		return qlz3_compress(source, destination, size,
							 (qlz3_state_compress *)state);

	}
	Insist(false);
}

static size_t
quicklz_decompressor(int level, const char *source, void *destination,
					 void *state)
{
	if (level == 1)
	{
		return qlz1_decompress(source, destination,
							   (qlz1_state_decompress *)state);
	}
	else if (level == 3)
	{
		return qlz3_decompress(source, destination,
							   (qlz3_state_decompress *)state);
	}
	Insist(false);
}

/* ---------------------------------------------------------------------
 * Quicklz constructor and destructor
 * ---------------------------------------------------------------------
 */
Datum
quicklz_constructor(PG_FUNCTION_ARGS)
{
	TupleDesc td 			= PG_GETARG_POINTER(0);
	StorageAttributes *sa	= PG_GETARG_POINTER(1);
	CompressionState *cs 	= palloc0(sizeof(CompressionState));
	quicklz_state *state	= palloc0(sizeof(quicklz_state));
	bool compress			= PG_GETARG_BOOL(2);
	size_t scratchlen		= 0;

	cs->opaque = (void *)state;

	Insist(PointerIsValid(td));
	Insist(PointerIsValid(sa->comptype));
	Insist(strcmp(sa->comptype, "quicklz") == 0);
	Insist(sa->complevel == 1 ||
		   sa->complevel == 3);

	state->level = sa->complevel;
	state->compress = compress;
	if (sa->complevel == 1)
	{
		state->compress_fn = quicklz_compressor;
		state->decompress_fn = quicklz_decompressor;
		if (compress)
			scratchlen = sizeof(qlz1_state_compress);
		else
			scratchlen = sizeof(qlz1_state_decompress);
	}
	else if (sa->complevel == 3)
	{
		state->compress_fn = quicklz_compressor;
	   	state->decompress_fn = quicklz_decompressor;

		if (compress)
			scratchlen = sizeof(qlz3_state_compress);
		else
			scratchlen = sizeof(qlz3_state_decompress);
	}
	else
		Insist(false); /* shouldn't get here but code defensively */

	state->scratch = palloc0(scratchlen);

	cs->desired_sz = quicklz_desired_sz;

	PG_RETURN_POINTER(cs);
}

Datum
quicklz_destructor(PG_FUNCTION_ARGS)
{
	CompressionState *cs = PG_GETARG_POINTER(0);
	quicklz_state *state = (quicklz_state *) cs->opaque;

	Insist(PointerIsValid(cs->opaque));

	pfree(state->scratch);
	pfree(cs->opaque);

	PG_RETURN_VOID();
}

/* ---------------------------------------------------------------------
 * SQL invokable compression and decompression routines for built in
 * compression algorithms. All routines have the same SQL signature:
 *
 * void fun(internal, int, internal, int, internal, internal)
 *
 * If we were to think of this as a C function it would be more like:
 *
 * void fun(void *src, size_t src_sz, void *dst, size_t dst_sz,
 *		  size_t *dst_used, void *opaque)
 *
 * The meaning of each argument is as follows:
 * src - A pointer to data to be compressed/decompressed
 * src_sz - The number of bytes to compress/decompress
 * dst - A pointer to pre-allocated memory. The data compressed or
 * 		 decompressed by the function are written here.
 * dst_sz - The amount of memory in bytes allocated at dst
 * dst_used - The number of bytes written. If dst_sz was too small to
 *			store the data, this is set to zero.
 * opaque - Internal to the compression function.
 */
Datum
quicklz_compress(PG_FUNCTION_ARGS)
{
	const void *src 		  = PG_GETARG_POINTER(0);
	int32 src_sz				= PG_GETARG_INT32(1);
	void *dst					  = PG_GETARG_POINTER(2);
	int32 dst_sz				= PG_GETARG_INT32(3);
	int32 *dst_used			  = PG_GETARG_POINTER(4);
	CompressionState *cs 	= (CompressionState *)PG_GETARG_POINTER(5);
	quicklz_state *state	= (quicklz_state *)cs->opaque;

	Insist(dst_sz >= quicklz_desired_sz(src_sz));

	*dst_used = state->compress_fn(state->level, src, dst, (size_t)src_sz,
								   state->scratch);

	PG_RETURN_VOID();
}

Datum
quicklz_decompress(PG_FUNCTION_ARGS)
{
	const char *src 	  	= PG_GETARG_POINTER(0);
	int32 src_sz				= PG_GETARG_INT32(1);
	void *dst					= PG_GETARG_POINTER(2);
	int32 dst_sz			 	= PG_GETARG_INT32(3);
	int32 *dst_used			  = PG_GETARG_POINTER(4);
	CompressionState *cs 	= (CompressionState *)PG_GETARG_POINTER(5);
	quicklz_state *state	= (quicklz_state *)cs->opaque;

	Insist(src_sz > 0 && dst_sz > 0);
	*dst_used = state->decompress_fn(state->level, src, dst, state->scratch);

	PG_RETURN_VOID();
}

Datum
quicklz_validator(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}

Datum
zlib_constructor(PG_FUNCTION_ARGS)
{
	TupleDesc		 td	   = PG_GETARG_POINTER(0);
	StorageAttributes *sa = PG_GETARG_POINTER(1);
	CompressionState *cs	   = palloc0(sizeof(CompressionState));
	zlib_state	   *state	= palloc0(sizeof(zlib_state));
	bool			  compress = PG_GETARG_BOOL(2);

	cs->opaque = (void *) state;
	cs->desired_sz = NULL;

	Insist(PointerIsValid(td));
	Insist(PointerIsValid(sa->comptype));

	if (sa->complevel == 0)
		sa->complevel = 1;

	state->level = sa->complevel;
	state->compress = compress;
	state->compress_fn = compress2;
	state->decompress_fn = uncompress;

	PG_RETURN_POINTER(cs);

}

Datum
zlib_destructor(PG_FUNCTION_ARGS)
{
	CompressionState *cs = PG_GETARG_POINTER(0);

	Insist(PointerIsValid(cs->opaque));
	pfree(cs->opaque);

	PG_RETURN_VOID();

}

Datum
zlib_compress(PG_FUNCTION_ARGS)
{
	const void	   *src	  = PG_GETARG_POINTER(0);
	int32			 src_sz   = PG_GETARG_INT32(1);
	void			 *dst	  = PG_GETARG_POINTER(2);
	int32			 dst_sz   = PG_GETARG_INT32(3);
	int32			*dst_used = PG_GETARG_POINTER(4);
	CompressionState *cs	   = (CompressionState *) PG_GETARG_POINTER(5);
	zlib_state	   *state	= (zlib_state *) cs->opaque;
	int				last_error;

	unsigned long amount_available_used = dst_sz;

	last_error = state->compress_fn((unsigned char *) dst,
									&amount_available_used, src, src_sz,
									state->level);

	*dst_used = amount_available_used;

	if (last_error != Z_OK)
	{
		switch (last_error)
		{
			case Z_MEM_ERROR:
				elog(ERROR, "out of memory");
				break;

			case Z_BUF_ERROR:
				/* 
				 * zlib returns this when it couldn't compressed the data
				 * to a size smaller than the input.
				 *
				 * The caller expects to detect this themselves so we set
				 * dst_used accordingly.
				 */
				*dst_used = src_sz;
				break;

			default:
				/* shouldn't get here */
				Insist(false);
				break;
		}
	}

	PG_RETURN_VOID();
}

Datum
zlib_decompress(PG_FUNCTION_ARGS)
{
	const char	   *src	= PG_GETARG_POINTER(0);
	int32			src_sz = PG_GETARG_INT32(1);
	void		   *dst	= PG_GETARG_POINTER(2);
	int32			dst_sz = PG_GETARG_INT32(3);
	int32		   *dst_used = PG_GETARG_POINTER(4);
	CompressionState *cs = (CompressionState *) PG_GETARG_POINTER(5);
	zlib_state	   *state = (zlib_state *) cs->opaque;
	int				last_error;
	unsigned long amount_available_used = dst_sz;

	Insist(src_sz > 0 && dst_sz > 0);


	last_error = state->decompress_fn(dst, &amount_available_used,
									  (const Bytef *) src, src_sz);

	*dst_used = amount_available_used;

	if (last_error != Z_OK)
	{
		switch (last_error)
		{
			case Z_MEM_ERROR:
				elog(ERROR, "out of memory");
				break;

			case Z_BUF_ERROR:

				/* 
				 * This would be a bug. We should have given a buffer big
				 * enough in the decompress case.
				 */
				elog(ERROR, "buffer size %d insufficient for compressed data",
					 dst_sz);
				break;

			case Z_DATA_ERROR:
				/*
				 * zlib data structures corrupted.
				 *
				 * Check out the error message: kind of like 'catalog
				 * convergence' for data corruption :-).
				 */
				elog(ERROR, "zlib encountered data in an unexpected format");

			default:
				/* shouldn't get here */
				Insist(false);
				break;
		}
	}

	PG_RETURN_VOID();
}

Datum
zlib_validator(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}

Datum
rle_type_constructor(PG_FUNCTION_ARGS)
{
	elog(ERROR, "rle_type block compression not supported");
	PG_RETURN_VOID();
}

Datum
rle_type_destructor(PG_FUNCTION_ARGS)
{
	elog(ERROR, "rle_type block compression not supported");
	PG_RETURN_VOID();
}

Datum
rle_type_compress(PG_FUNCTION_ARGS)
{
	elog(ERROR, "rle_type block compression not supported");
	PG_RETURN_VOID();
}

Datum
rle_type_decompress(PG_FUNCTION_ARGS)
{
	elog(ERROR, "rle_type block compression not supported");
	PG_RETURN_VOID();
}

Datum
rle_type_validator(PG_FUNCTION_ARGS)
{
	elog(ERROR, "rle_type block compression not supported");
	PG_RETURN_VOID();
}

/* Dummy routines to implement compresstype=none */
Datum
dummy_compression_constructor(PG_FUNCTION_ARGS)
{
	elog(ERROR, "dummy compression called directly");
	PG_RETURN_VOID();
}

Datum
dummy_compression_destructor(PG_FUNCTION_ARGS)
{
	elog(ERROR, "dummy compression called directly");
	PG_RETURN_VOID();
}

Datum
dummy_compression_compress(PG_FUNCTION_ARGS)
{
	elog(ERROR, "dummy compression called directly");
	PG_RETURN_VOID();
}

Datum
dummy_compression_decompress(PG_FUNCTION_ARGS)
{
	elog(ERROR, "dummy compression called directly");
	PG_RETURN_VOID();
}

Datum
dummy_compression_validator(PG_FUNCTION_ARGS)
{
	elog(ERROR, "dummy compression called directly");
	PG_RETURN_VOID();
}

/*
 * Does a compression algorithm exist by the name of `compresstype'?
 */
bool
compresstype_is_valid(char *comptype)
{
	NameData	compname;
	bool		found = false;

	compname = comptype_to_name(comptype);

	found = (0 !=
			 caql_getcount(
					 NULL,
					 cql("SELECT COUNT(*) FROM pg_compression "
						 " WHERE compname = :1 ",
						 NameGetDatum(&compname))));
		
	return found;
}

/* 
 * Make encoding (compresstype = none, blocksize=...). We do this for the case
 * where the user has not specified an encoding for the column.
 */
List *
default_column_encoding_clause(void)
{
	DefElem *e1 = makeDefElem("compresstype", (Node *)makeString("none"));
	DefElem *e2 = makeDefElem("blocksize",
					(Node *)makeInteger(DEFAULT_APPENDONLY_BLOCK_SIZE));
	DefElem *e3 = makeDefElem("compresslevel",
							  (Node *)makeInteger(0));

	return list_make3(e1, e2, e3);
}

bool
is_storage_encoding_directive(char *name)
{
	int i = 0;

	while (storage_directive_names[i])
	{
		if (strcmp(name, storage_directive_names[i]) == 0)
			return true;
		i++;
	}
	return false;
}

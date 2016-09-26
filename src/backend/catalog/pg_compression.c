/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * ---------------------------------------------------------------------
 *
 * Interfaces to low level compression functionality.
 */

#include "postgres.h"
#include "fmgr.h"

#include "access/genam.h"
#include "access/reloptions.h"
#include "access/tupdesc.h"
#include "access/tupmacs.h"
#include "catalog/catquery.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_compression.h"
#include "catalog/dependency.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbappendonlystoragelayer.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "storage/gp_compress.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#include "snappy-c.h"

/* names we expect to see in ENCODING clauses */
char *storage_directive_names[] = {"compresstype", "compresslevel",
								   "blocksize", NULL};

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

	/*
	 * This is a hack: We added the snappy support for row oriented storage, however
	 * to make the feature friendly to upgradation in short term, we decide to not
	 * modify the system table to implement this. Following ugly hack is to
	 * complete this. Let's remove this piece of code after snappy support is
	 * added to the related system tables.
	 */
	if (strcmp(NameStr(compname), "snappy") == 0)
	{
		funcs = palloc0(sizeof(PGFunction) * NUM_COMPRESS_FUNCS);
		funcs[COMPRESSION_CONSTRUCTOR] = snappy_constructor;
		funcs[COMPRESSION_DESTRUCTOR] = snappy_destructor;
		funcs[COMPRESSION_COMPRESS] = snappy_compress_internal;
		funcs[COMPRESSION_DECOMPRESS] = snappy_decompress_internal;
		funcs[COMPRESSION_VALIDATOR] = snappy_validator;
		return funcs;
	}

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
snappy_constructor(PG_FUNCTION_ARGS)
{
	TupleDesc			td = PG_GETARG_POINTER(0);
	StorageAttributes	*sa = PG_GETARG_POINTER(1);
	CompressionState	*cs	= palloc0(sizeof(CompressionState));

	cs->opaque = NULL;
	cs->desired_sz = snappy_max_compressed_length;

	Insist(PointerIsValid(td));
	Insist(PointerIsValid(sa->comptype));

	PG_RETURN_POINTER(cs);
}

Datum
snappy_destructor(PG_FUNCTION_ARGS)
{
	CompressionState	*cs = PG_GETARG_POINTER(0);

	if (cs->opaque)
	{
		Insist(PointerIsValid(cs->opaque));
		pfree(cs->opaque);
	}

	PG_RETURN_VOID();
}

static void
elog_snappy_error(snappy_status retval, char *func_name,
				  int src_sz, int dst_sz, int dst_used)
{
	switch (retval)
	{
		case SNAPPY_INVALID_INPUT:
			elog(ERROR, "invalid input for %s(): "
				 "src_sz=%d dst_sz=%d dst_used=%d",
				 func_name, src_sz, dst_sz, dst_used);
			break;
		case SNAPPY_BUFFER_TOO_SMALL:
			elog(ERROR, "buffer is too small in %s(): "
				 "src_sz=%d dst_sz=%d dst_used=%d",
				 func_name, src_sz, dst_sz, dst_used);
			break;
		default:
			elog(ERROR, "unknown failure (return value %d) for %s(): "
				 "src_sz=%d dst_sz=%d dst_used=%d", retval, func_name,
				 src_sz, dst_sz, dst_used);
			break;
	}
}

Datum
snappy_compress_internal(PG_FUNCTION_ARGS)
{
	const char		*src = PG_GETARG_POINTER(0);
	size_t			src_sz = PG_GETARG_INT32(1);
	char			*dst = PG_GETARG_POINTER(2);
	size_t			dst_sz = PG_GETARG_INT32(3);
	int32			*dst_used = PG_GETARG_POINTER(4);
	size_t			compressed_length;
	snappy_status	retval;

	compressed_length = snappy_max_compressed_length(src_sz);
	Insist(dst_sz >= compressed_length);

	retval = snappy_compress(src, src_sz, dst, &compressed_length);
	*dst_used = compressed_length;

	if (retval != SNAPPY_OK)
		elog_snappy_error(retval, "snappy_compress", src_sz, dst_sz, *dst_used);

	PG_RETURN_VOID();
}

Datum
snappy_decompress_internal(PG_FUNCTION_ARGS)
{
	const char		*src	= PG_GETARG_POINTER(0);
	size_t			src_sz = PG_GETARG_INT32(1);
	char			*dst	= PG_GETARG_POINTER(2);
	size_t			dst_sz = PG_GETARG_INT32(3);
	int32			*dst_used = PG_GETARG_POINTER(4);
	size_t			uncompressed_length;
	snappy_status	retval;

	Insist(src_sz > 0 && dst_sz > 0);

	retval = snappy_uncompressed_length(src, src_sz,
										&uncompressed_length);
	if (retval != SNAPPY_OK)
		elog_snappy_error(retval, "snappy_uncompressed_length",
						  src_sz, dst_sz, *dst_used);

	Insist(dst_sz >= uncompressed_length);

	retval = snappy_uncompress(src, src_sz, dst, &uncompressed_length);
	*dst_used = uncompressed_length;

	if (retval != SNAPPY_OK)
		elog_snappy_error(retval, "snappy_uncompressed",
						  src_sz, dst_sz, *dst_used);

	PG_RETURN_VOID();
}

Datum
snappy_validator(PG_FUNCTION_ARGS)
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

	/*
	 * FIXME: This is a hack. Should implement related handlers and register 
	 * in system tables instead. snappy handlers have already been implemented
	 * but not registerd in system tables (see comment in GetCompressionImplement()
	 * for details).
	 */
	if(!found)
	{
		if(strcmp(comptype, "snappy") == 0 || strcmp(comptype, "gzip") == 0)
			found = true;
	}

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

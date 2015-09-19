/*-------------------------------------------------------------------------
 *
 * reloptions.h
 *	  Core support for relation options (pg_class.reloptions)
 *
 * Note: the functions dealing with text-array reloptions values declare
 * them as Datum, not ArrayType *, to avoid needing to include array.h
 * into a lot of low-level code.
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/access/reloptions.h,v 1.2 2006/10/04 00:30:07 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELOPTIONS_H
#define RELOPTIONS_H

#include "access/htup.h"
#include "nodes/pg_list.h"
#include "utils/rel.h"

extern Datum transformRelOptions(Datum oldOptions, List *defList,
					bool ignoreOids, bool isReset);

extern List *untransformRelOptions(Datum options);

extern void parseRelOptions(Datum options, int numkeywords,
				const char *const * keywords,
				char **values, bool validate);

extern bytea *default_reloptions(Datum reloptions, bool validate, char relkind,
				   int minFillfactor, int defaultFillfactor);

extern void heap_test_override_reloptions(char relkind, StdRdOptions *stdRdOptions, int *safewrite);

extern bytea *heap_reloptions(char relkind, Datum reloptions, bool validate);

extern bytea *index_reloptions(RegProcedure amoptions, Datum reloptions,
				 bool validate);

extern TidycatOptions *tidycat_reloptions(Datum reloptions);

extern void validateAppendOnlyRelOptions(bool ao, int blocksize,
										 int pagesize, int rowgroupsize,
										 int writesize,
										 int complevel, char* comptype, 
										 bool checksum, char relkind, char colstore);

#endif   /* RELOPTIONS_H */

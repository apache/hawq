/*-------------------------------------------------------------------------
 *
 * cdbrelsize.h
 *
 * Get the max size of the relation across the segDBs
 *
 * Copyright (c) 2006-2008, Greenplum inc
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBRELSIZE_H_
#define CDBRELSIZE_H_

void clear_relsize_cache(void);

int64 cdbRelSize(Relation rel);


#endif /*CDBRELSIZE_H_*/

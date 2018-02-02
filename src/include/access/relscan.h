/*-------------------------------------------------------------------------
 *
 * relscan.h
 *	  POSTGRES relation scan descriptor definitions.
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/access/relscan.h,v 1.50 2006/10/04 00:30:07 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELSCAN_H
#define RELSCAN_H

#include "access/formatter.h"
#include "access/skey.h"
#include "access/memtup.h"
#include "access/aosegfiles.h"
#include "access/plugstorage_utils.h"
#include "nodes/plannodes.h"
#include "storage/bufpage.h"
#include "utils/tqual.h"

/* forward declaration from nodes/execnodes.h */
typedef struct ScanState ScanState;

typedef struct HeapScanDescData
{
	/* scan parameters */
	Relation	rs_rd;			/* heap relation descriptor */
	Snapshot	rs_snapshot;	/* snapshot to see */
	int			rs_nkeys;		/* number of scan keys */
	ScanKey		rs_key;			/* array of scan key descriptors */
	BlockNumber rs_nblocks;		/* number of blocks to scan */
	bool		rs_pageatatime; /* verify visibility page-at-a-time? */

	/* scan current state */
	bool		rs_inited;		/* false = scan not init'd yet */
	HeapTupleData rs_ctup;		/* current tuple in scan, if any */
	BlockNumber rs_cblock;		/* current block # in scan, if any */
	Buffer		rs_cbuf;		/* current buffer in scan, if any */
	/* NB: if rs_cbuf is not InvalidBuffer, we hold a pin on that buffer */
	ItemPointerData rs_mctid;	/* marked scan position, if any */

	/* these fields only used in page-at-a-time mode and for bitmap scans */
	int			rs_cindex;		/* current tuple's index in vistuples */
	int			rs_mindex;		/* marked tuple's saved index */
	int			rs_ntuples;		/* number of visible tuples on page */
	OffsetNumber rs_vistuples[MaxHeapTuplesPerPage];	/* their offsets */

	struct {
		BlockNumber block;
		Buffer	    buffer;
	} rs_rahead[16];
} HeapScanDescData;

typedef HeapScanDescData *HeapScanDesc;

/*
 * We use the same IndexScanDescData structure for both amgettuple-based
 * and amgetbitmap-based index scans.  Some fields are only relevant in
 * amgettuple-based scans.
 */
typedef struct IndexScanDescData
{
	/* scan parameters */
	Relation	heapRelation;	/* heap relation descriptor, or NULL */
	Relation	indexRelation;	/* index relation descriptor */
	Snapshot	xs_snapshot;	/* snapshot to see */
	int			numberOfKeys;	/* number of scan keys */
	ScanKey		keyData;		/* array of scan key descriptors */
	bool		is_multiscan;	/* TRUE = using amgetmulti */

	/* signaling to index AM about killing index tuples */
	bool		kill_prior_tuple;		/* last-returned tuple is dead */
	bool		ignore_killed_tuples;	/* do not return killed entries */

	/* index access method's private state */
	void	   *opaque;			/* access-method-specific info */
	
	/* these fields are used by some but not all AMs: */
	ItemPointerData currentItemData;	/* current index pointer */
	ItemPointerData currentMarkData;	/* marked position, if any */

	/*
	 * xs_ctup/xs_cbuf are valid after a successful index_getnext. After
	 * index_getnext_indexitem, xs_ctup.t_self contains the heap tuple TID
	 * from the index entry, but its other fields are not valid.
	 */
	HeapTupleData xs_ctup;		/* current heap tuple, if any */
	Buffer		xs_cbuf;		/* current heap buffer in scan, if any */
	/* NB: if xs_cbuf is not InvalidBuffer, we hold a pin on that buffer */
} IndexScanDescData;

typedef IndexScanDescData *IndexScanDesc;

/*
 * used for scan of external relations
 */
typedef struct FileScanDescData
{
	/* scan parameters */
	Relation	fs_rd;			/* target relation descriptor */
	Index       fs_scanrelid;
	FILE	   *fs_file;		/* the file pointer to our URI */
	char	   *fs_uri;			/* the URI string */
	bool		fs_noop;		/* no op. this segdb has no file to scan */
	uint32      fs_scancounter;	/* copied from struct ExternalScan in plan */
	List	   *fs_scanquals;	/* referenced scan qualifier list */
	
	/* current file parse state */
	struct CopyStateData *fs_pstate;

	Form_pg_attribute *attr;
	AttrNumber	num_phys_attrs;
	Datum	   *values;
	bool	   *nulls;
	int		   *attr_offsets;
	FmgrInfo   *in_functions;
	Oid		   *typioparams;
	Oid			in_func_oid;
	ErrorContextCallback errcontext;
	
	/* current file scan state */
	bool		fs_inited;		/* false = scan not init'd yet */
	TupleDesc	fs_tupDesc;
	HeapTupleData fs_ctup;		/* current tuple in scan, if any */
	Buffer		fs_cbuf;		/* always invalid buffer */

	/* custom data formatter */
	FormatterData *fs_formatter;
	
	/* formatter type and name */
	int fs_formatter_type;
	char *fs_formatter_name;

	/* current scan information for pluggable format */
	PlugStorageScanFuncs fs_ps_scan_funcs;   /* scan functions */
	void *fs_ps_user_data;                   /* user data */
	ScanState *fs_ps_scan_state;             /* support rescan */
	Plan *fs_ps_plan;                        /* support rescan */

}	FileScanDescData;

typedef FileScanDescData *FileScanDesc;

/*
 * used for scan of append only relations using BufferedRead and VarBlocks
 * Defined in cdb/cdbappendonlyam.h
 */

/*
  typedef AppendOnlyScanDescData *AppendOnlyScanDesc;
 */

/*
 * HeapScanIsValid
 *		True iff the heap scan is valid.
 */
#define HeapScanIsValid(scan) PointerIsValid(scan)

/*
 * IndexScanIsValid
 *		True iff the index scan is valid.
 */
#define IndexScanIsValid(scan) PointerIsValid(scan)

#endif   /* RELSCAN_H */

/*-------------------------------------------------------------------------
 *
 * cdbsrlz.c
 *	  Serialize a PostgreSQL sequential plan tree.
 *
 * Copyright (c) 2004-2008, Greenplum inc
 *
 * NOTES
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "regex/regex.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "optimizer/clauses.h"
#include "cdb/cdbplan.h"
#include "cdb/cdbsrlz.h"
#include "nodes/print.h"
#include "cdb/cdbinmemheapam.h"
#include "libpq/pqformat.h"
#include "cdb/cdbvars.h"
#include "access/xact.h"
#include "cdb/cdbtm.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace.h"

#include "utils/inval.h"

#include "cdb/cdbdispatchedtablespaceinfo.h"

#include <math.h>
#ifdef HAVE_LIBZ
#include <zlib.h>
#endif

char *WriteBackCatalogs = NULL;
int32 WriteBackCatalogLen = 0;

static char *compress_string(const char *src, int uncompressed_size, int *size);
static char *uncompress_string(const char *src, int size, int * uncompressed_len);

/*
 * compressBound doesn't exist in older zlibs, so let's use our own
 */
static
unsigned long 
gp_compressBound(unsigned long sourceLen)
{
	return sourceLen + (sourceLen >> 12) + (sourceLen >> 14) + 11;
}

/*
 * serializeNode -
 * This is used on the query dispatcher to serialize Plan and Query Trees for
 * dispatching to qExecs.
 * The returned string is palloc'ed in the current memory context.
 */
char *
serializeNode(Node *node, int *size)
{
	char *pszNode;
	char *sNode;
	int uncompressed_size;

	Assert(node != NULL);
	Assert(size != NULL);

	pszNode = nodeToBinaryStringFast(node, &uncompressed_size);
	Assert(pszNode != NULL);

	sNode = compress_string(pszNode, uncompressed_size, size);
	pfree(pszNode);

	if (DEBUG5 >= log_min_messages)
	{
		Node * newnode = NULL;
		PG_TRY();
			{
				newnode = deserializeNode(sNode, *size);
		}
		PG_CATCH();
			{
				elog_node_display(DEBUG5, "Before serialization", node, true );
				PG_RE_THROW();
		}
		PG_END_TRY();

		/* Some plans guarantee these differences (see serialization
		 * of plan nodes -- they avoid sending QD-only info out) */
		if (strcmp(nodeToString(node), nodeToString(newnode)) != 0)
		{
			elog_node_display(DEBUG5, "Before serialization", node, true );

			elog_node_display(DEBUG5, "After deserialization", newnode, true );
		}
	}

	return sNode;
}

/*
 * deserializeNode -
 * This is used on the qExecs to deserialize serialized Plan and Query Trees
 * received from the dispatcher.
 * The returned node is palloc'ed in the current memory context.
 */
Node *
deserializeNode(const char *strNode, int size)
{
	char *sNode;
	Node *node;
	int uncompressed_len;

	Assert(strNode != NULL);

	sNode = uncompress_string(strNode, size, &uncompressed_len);

	Assert(sNode != NULL);

	node = readNodeFromBinaryString(sNode, uncompressed_len);

	pfree(sNode);

	return node;
}

/*
 * Compress a (binary) string using zlib.
 * 
 * returns the compressed data and the size of the compressed data.
 */
static char *
compress_string(const char *src, int uncompressed_size, int *size)
{
	int level = 3;
	unsigned long compressed_size;
	int status;

	Bytef * result;

	Assert(size!=NULL);

	if (src == NULL)
	{
		*size = 0;
		return NULL ;
	}

	compressed_size = gp_compressBound(uncompressed_size); /* worst case */

	result = palloc(compressed_size + sizeof(int));
	memcpy(result, &uncompressed_size, sizeof(int)); 		/* save the original length */

	status = compress2(result+sizeof(int), &compressed_size, (Bytef *)src, uncompressed_size, level);
	if (status != Z_OK)
		elog(ERROR,"Compression failed: %s (errno=%d) uncompressed len %d, compressed %d",
				zError(status), status, uncompressed_size, (int)compressed_size);

	*size = compressed_size + sizeof(int);
	elog(DEBUG2,"Compressed from %d to %d ", uncompressed_size, *size);

	return (char *) result;
}

/*
 * Uncompress the binary string 
 */
static char *
uncompress_string(const char *src, int size, int *uncompressed_len)
{
	Bytef * result;
	unsigned long resultlen;
	int status;
	*uncompressed_len = 0;

	if (src == NULL )
		return NULL ;

	Assert(size >= sizeof(int));

	memcpy(uncompressed_len, src, sizeof(int));

	resultlen = *uncompressed_len;
	result = palloc(resultlen);

	status = uncompress(result, &resultlen, (Bytef *)(src+sizeof(int)), size-sizeof(int));
	if (status != Z_OK)
		elog(ERROR,"Uncompress failed: %s (errno=%d compressed len %d, uncompressed %d)",
				zError(status), status, size, *uncompressed_len);

	return (char *) result;
}

/*
 * serialize all modified tuple in in-memory heap table.
 */
static void
serializeInMemHeapTable(InMemHeapRelation rel, StringInfo buffer)
{
	int32 i, tupsize = 0;

	/* add oid of table first */
	pq_sendint(buffer, rel->relid, sizeof(Oid));

	for (i = 0; i < rel->tupsize; ++i)
	{
		/*
		 * On QE, at the end of each statement, invalidate all dispatched catalogs in cache.
		 *
		 * Master may update the dispatched catalog after the current statement,
		 * and there is no way to notify.
		 */
		if(Gp_role == GP_ROLE_EXECUTE && NULL != rel->rel)
		{
			CacheInvalidateHeapTuple(rel->rel, rel->tuples[i].tuple);
		}

		if (rel->tuples[i].flags == INMEM_HEAP_TUPLE_UPDATED)
		{
			++tupsize;
		}
	}

	/* add tuple size */
	pq_sendint(buffer, tupsize, sizeof(int32));

	/* for each tuple */
	for (i = 0; i < rel->tupsize; ++i)
	{
		if ( rel->tuples[i].flags == INMEM_HEAP_TUPLE_UPDATED)
		{

			if (Gp_role != GP_ROLE_EXECUTE)
				Assert(ItemPointerIsValid(&rel->tuples[i].tuple->t_self));

			/* add tuple contentid */
			pq_sendint(buffer, rel->tuples[i].contentid, sizeof(int32));
			/* add tuple data length */
			pq_sendint(buffer, rel->tuples[i].tuple->t_len, sizeof(uint32));
			/* add flag */
			pq_sendint(buffer, rel->tuples[i].flags, sizeof(int8));
			/* finally copy heap tuple */
			pq_sendbytes(buffer, (const char *) rel->tuples[i].tuple,
					HEAPTUPLESIZE + rel->tuples[i].tuple->t_len);
		}
	}
}

/*
 * serialize all in-memory heap
 */
char *
serializeInMemCatalog(int * len)
{
	HASH_SEQ_STATUS scan;
	StringInfoData str;
	char * retval;
	struct OidInMemHeapMappingEntry * entry;

	int i;
	HASHCTL info;
	HTAB * baseHash = NULL;
	bool found;


	initStringInfo(&str);
	/*
	 * first, serialize master's local transaction id
	 */
	pq_sendint(&str, GetMasterTransactionId(), sizeof(TransactionId));

	if (NULL !=  OidInMemHeapMapping)
	{
		/*
		 * to deserialize a catalog, segments need to open the relation first,
		 * for some catalog such as pg_aoseg_XXX, it cannot be open before other
		 * base catalog get ready.
		 * therefore we need serialize base catalog first,
		 * if it is in the OidInMemHeapMappingEntry.
		 */
		int32 base[] = { AttributeRelationId, RelationRelationId,
				NamespaceRelationId, TableSpaceRelationId, TypeRelationId,
				ProcedureRelationId };

		MemSet(&info, 0, sizeof(info));

		info.hash = int32_hash;
		info.keysize = sizeof(int32);
		info.entrysize = sizeof(int32);

		baseHash = hash_create("base catalog hash",
				sizeof(base) / sizeof(base[0]), &info, HASH_FUNCTION | HASH_ELEM);

		for (i = 0; i < sizeof(base) / sizeof(base[0]); ++i)
		{
			entry = hash_search(OidInMemHeapMapping, &base[i], HASH_FIND, &found);
			if (found)
			{
				Assert(entry != NULL && base[i] == entry->relid);
				/*
				 * base catalog base[i] is in OidInMemHeapMapping,
				 * serialize it first and add to base catalog hash.
				 */
				serializeInMemHeapTable(entry->rel, &str);

				hash_search(baseHash, &base[i], HASH_ENTER, &found);
				Assert(FALSE == found);
			}
		}

		hash_seq_init(&scan, OidInMemHeapMapping);

		/*
		 * serialize other catalog which is not a base catalog.
		 */
		while (!!(entry = (struct OidInMemHeapMappingEntry *) hash_seq_search(&scan)))
		{
			hash_search(baseHash, &entry->rel->relid, HASH_FIND, &found);
			if (!found)
				serializeInMemHeapTable(entry->rel, &str);
		}

		hash_destroy(baseHash);
	}

	retval = compress_string(str.data, str.len, len);

	pfree(str.data);

	return retval;
}

/*
 * rebuild tablespace location info on QE
 */
static void
deserializeAndRebuildTablespaceInfo(StringInfo msg)
{
	int i;
	int32 tuplesize;
	Oid tablespace;

	char *str;
	char path[FilespaceLocationBlankPaddedWithNullTermLen];
	int slen;

	tuplesize = (int32) pq_getmsgint(msg, sizeof(uint32));

	for (i = 0; i < tuplesize; ++i)
	{
		tablespace = (Oid) pq_getmsgint(msg, sizeof(Oid));

		str = &msg->data[msg->cursor];

		slen = strlen(str);
		if (msg->cursor + slen >= msg->len)
			elog(ERROR, "invalid string in message from QD");

		msg->cursor += slen + 1;

		Assert(Gp_role == GP_ROLE_EXECUTE);
		Assert(NULL != strstr(str, "%d"));
		snprintf(path, FilespaceLocationBlankPaddedWithNullTermLen, str, GpIdentity.segindex);

		DispatchedFilespace_AddForTablespace(tablespace, path);
	}


}

/*
 * deserialize dispatched catalogs on QE
 */
void
deserializeAndUpdateCatalog(const char * catalog, int size)
{
	int s, i, contentid, tuplesize, tuplelen;
	uint8 flags;
	char * p;
	Oid relid;
	HeapTuple tuple;
	StringInfoData str;
	Relation rel;

	TransactionId xid;

	if (Gp_role == GP_ROLE_UTILITY || IsBootstrapProcessingMode())
		return;

	Assert(Gp_role == GP_ROLE_DISPATCH);

	p = uncompress_string(catalog, size, &s);
	initStringInfoOfString(&str, p, s);

	/*
	 * get master's transaction id.
	 */
	xid = (TransactionId) pq_getmsgint(&str, sizeof(TransactionId));
	Assert(xid == GetCurrentTransactionId());

	while (str.cursor < str.len)
	{
		relid = (Oid) pq_getmsgint(&str, sizeof(Oid));
		Assert(InvalidOid != relid);

		Assert(relid != TableSpaceRelationId);

		tuplesize = (int) pq_getmsgint(&str, sizeof(int32));
		Assert(tuplesize >= 0);

		rel = heap_open(relid, RowExclusiveLock);

		for (i = 0; i < tuplesize; ++i)
		{
			contentid = (int) pq_getmsgint(&str, sizeof(int32));

			tuplelen = (uint32) pq_getmsgint(&str, sizeof(uint32));

			flags = (uint8) pq_getmsgint(&str, sizeof(uint8));

			tuple = (HeapTuple) pq_getmsgbytes(&str, HEAPTUPLESIZE + tuplelen);
			tuple->t_data = (HeapTupleHeader) ((char *) tuple + HEAPTUPLESIZE);

            if (flags == INMEM_HEAP_TUPLE_UPDATED)
            {
                simple_heap_update(rel, &tuple->t_self, tuple);
                CatalogUpdateIndexes(rel, tuple);
            }
		}
		heap_close(rel, RowExclusiveLock);
	}
}

/*
 * deserialize sent back catalog and update on QD
 */
void
deserializeAndRebuildInmemCatalog(const char * catalog, int size)
{
	int s, i, tuplelen, contentid;
	int32 tuplesize;
	uint8 flags;
	char * p;
	Oid relid;
	HeapTuple tuple;
	StringInfoData str;
	Relation rel;
	InMemHeapRelation inmemrel;

	TransactionId xid;

	Assert(Gp_role == GP_ROLE_EXECUTE);

	p = uncompress_string(catalog, size, &s);
	initStringInfoOfString(&str, p, s);

	/*
	 * get master's transaction id.
	 */
	xid = (TransactionId) pq_getmsgint(&str, sizeof(TransactionId));
	SetMasterTransactionId(xid);

	while (str.cursor < str.len)
	{
		relid = (Oid) pq_getmsgint(&str, sizeof(Oid));
		Assert(InvalidOid != relid);

		if (TableSpaceRelationId == relid)
		{
			deserializeAndRebuildTablespaceInfo(&str);
			continue;
		}

		tuplesize = (int32) pq_getmsgint(&str, sizeof(int32));
		Assert(tuplesize >= 0);

		inmemrel = OidGetInMemHeapRelation(relid);
		if (NULL == inmemrel)
		{
			rel = heap_open(relid, AccessShareLock);
			inmemrel = InMemHeap_Create(relid, rel, TRUE, 10, AccessShareLock, RelationGetRelationName(rel));
			Assert(NULL != inmemrel);
		}

		for (i = 0; i < tuplesize; ++i)
		{

			contentid = (int) pq_getmsgint(&str, sizeof(int32));

			tuplelen = (uint32) pq_getmsgint(&str, sizeof(uint32));

			if (contentid != GpIdentity.segindex && contentid != -1)
			{
				int temp = sizeof(uint8) + HEAPTUPLESIZE + tuplelen;
				pq_getmsgbytes(&str, temp);
				continue;
			}

			flags = (uint8) pq_getmsgint(&str, sizeof(uint8));

			tuple = (HeapTuple) pq_getmsgbytes(&str, HEAPTUPLESIZE + tuplelen);
			tuple->t_data = (HeapTupleHeader) ((char *) tuple + HEAPTUPLESIZE);

			Assert(tuple->t_len == tuplelen);
			Assert(ItemPointerIsValid(&tuple->t_self));

			InMemHeap_Insert(inmemrel, tuple, contentid);
		}
	}
}

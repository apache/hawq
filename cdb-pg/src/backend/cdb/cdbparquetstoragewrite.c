/*
 * cdbparquetstoragewrite.c
 *
 *  Created on: Jul 30, 2013
 *      Author: malili
 */
#include "cdb/cdbparquetstoragewrite.h"
#include "lib/stringinfo.h"
#include "utils/cash.h"
#include "utils/geo_decls.h"
#include "utils/date.h"
#include "utils/numeric.h"
#include "utils/xml.h"
#include "utils/inet.h"
#include "access/catquery.h"

#include "snappy-c.h"
#include "zlib.h"


static void addDataPage(
		ParquetColumnChunk columnChunk);

static int encodeCurrentPage(
		ParquetColumnChunk chunk);

static int finalizeCurrentAndNewPage(
		ParquetColumnChunk columnChunk);

static void flushDataPage(
		ParquetColumnChunk chunk,
		int page_number);

static void initGroupType(
		FileField_4C *field,
		char *name,
		RepetitionType repetitionType,
		int hawqType,
		int r,
		int d,
		int depth,
		int numChildren,
		char *parentPathInSchema);

static void initPrimitiveType(
		FileField_4C *field,
		char *name,
		RepetitionType repetitionType,
		int hawqType,
		int typeLen,
		int r,
		int d,
		int depth,
		char *parentPathInSchema);

/* init embedded data types */
static void initPointType(
		FileField_4C *pointField,
		Form_pg_attribute att);

static void initPathType(
		FileField_4C *pathField,
		Form_pg_attribute att);

static void initLsegBoxType(
		FileField_4C *lsegBoxField,
		Form_pg_attribute att);

static void initPolygonType(
		FileField_4C *polygonField,
		Form_pg_attribute att);

static void initCircleType(
		FileField_4C *circleField,
		Form_pg_attribute att);

static void addSingleColumn(
		AppendOnlyEntry *catalog,
		struct ColumnChunkMetadata_4C** columnsMetadata,
		ParquetColumnChunk columns,
		int maxChunkLimitSize,
		struct FileField_4C *field,
		int *colIndex,
		File parquetFile);

static int appendNullForFields(
		struct FileField_4C *field,
		ParquetColumnChunk columnChunks,
		int *colIndex);

static int appendValueForFields(
		struct FileField_4C *field,
		ParquetColumnChunk columnChunks,
		int *colIndex,
		Datum value);

static int appendParquetColumnNull(
		ParquetColumnChunk columnChunk);

static int appendParquetColumnValue(
		ParquetColumnChunk columnChunk,
		Datum value,
		int r,
		int d);

/*----------------------------------------------------------------
 * append column of geometric types 
 * - point, lseg, path, box, polygon, circle
 *----------------------------------------------------------------*/
static int appendParquetColumn_Point(
		ParquetColumnChunk columnChunks,
		int *colIndex,
		Point *point,
		int r,
		int d);

static int appendParquetColumn_Lseg(
		ParquetColumnChunk columnChunks,
		int *colIndex,
		LSEG *lseg,
		int r,
		int d);

static int appendParquetColumn_Path(
		ParquetColumnChunk columnChunks,
		int *colIndex,
		PATH *path,
		int r,
		int d);

static int appendParquetColumn_Box(
		ParquetColumnChunk columnChunks,
		int *colIndex,
		BOX *box,
		int r,
		int d);

static int appendParquetColumn_Polygon(
		ParquetColumnChunk columnChunks,
		int *colIndex,
		POLYGON *polygon,
		int r,
		int d);

static int appendParquetColumn_Circle(
		ParquetColumnChunk columnChunks,
		int *colIndex,
		CIRCLE *circle,
		int r,
		int d);

static char *generateHAWQSchemaStr(
		ParquetFileField pfields,
		int fieldCount);

static char *getTypeName(Oid typeOid);

static int encodePlain(
		Datum data,
		ParquetDataPage current_page,
		int hawqTypeId,
		int maxPageSize,
		int maxPageLimitSize);

static int approximatePageSize(ParquetDataPage page);

static bool exceedsPageSizeLimit(ParquetDataPage current_page,
		int currentPageSize, int maxPageSize, int maxPageLimitSize);

#define ENCODE_INVALID_VALUE	-1
#define ENCODE_OUTOF_PAGE		-2

/**
 * generate hawq schema in to string. for example:
 *
 *	message person {
 * 		required varchar name;
 * 		required int2 age;
 * 		optional group home_addr (address) {
 * 			required varchar street;
 * 			required varchar city;
 * 			required varchar state;
 * 			required int4 zip;
 * 		}
 * 		required varchar[] tags;
 * 	}

 What about Array?
 */
static char *
generateHAWQSchemaStr(ParquetFileField pfields,
					  int fieldCount)
{
	StringInfo schemaBuf = makeStringInfo();
	appendStringInfo(schemaBuf, "message hawqschema {");

	for (ParquetFileField field = pfields; field < pfields + fieldCount; field++)
	{
		/* FIXME add ARRAY and UDF type support */
		char *typeName = getTypeName(field->hawqTypeId);
		appendStringInfo(schemaBuf, "%s %s %s;",
						 (field->repetitionType == REQUIRED) ? "required" : "optional",
						 typeName,
						 field->name);
		pfree(typeName);
	}

	appendStringInfo(schemaBuf, "}");
	return schemaBuf->data;
}

/**
 * point(600)			group {required double x; required double y;}
 *
 * lseg(601)			group {required double x1; required double y1; required double x2; required double y2;}
 *
 * box(603)				group {required double x1; required double y1; required double x2; required double y2;}
 *
 * circle(718)			group {required double x; required double y; required double r;}
 *
 * path(602)			group {	repeated group {required double x; required double y;}}
 *
 */
int
initparquetMetadata(ParquetMetadata parquetmd,
					TupleDesc tableAttrs,
					File parquetFile)
{
	Form_pg_attribute att;
	ParquetFileField field;

	parquetmd->version = CURRENT_PARQUET_VERSION;
	parquetmd->fieldCount = tableAttrs->natts;
	parquetmd->pfield = palloc0(tableAttrs->natts * sizeof(struct FileField_4C));
	int colCount = 0;
	int schemaTreeNodeCount = 0;
	for (int i = 0; i < tableAttrs->natts; i++)
	{
		att = tableAttrs->attrs[i];
		field = &parquetmd->pfield[i];

		switch (att->atttypid)
		{
		/** basic types */
		case HAWQ_TYPE_BOOL:
		case HAWQ_TYPE_BYTE:
		case HAWQ_TYPE_INT2:
		case HAWQ_TYPE_INT4:
		case HAWQ_TYPE_MONEY:
		case HAWQ_TYPE_INT8:
		case HAWQ_TYPE_FLOAT4:
		case HAWQ_TYPE_FLOAT8:
		case HAWQ_TYPE_NUMERIC:
		/* text related types */
		case HAWQ_TYPE_NAME:
		case HAWQ_TYPE_CHAR:
		case HAWQ_TYPE_BPCHAR:
		case HAWQ_TYPE_VARCHAR:
		case HAWQ_TYPE_TEXT:
		case HAWQ_TYPE_XML:
		/* time related types */
		case HAWQ_TYPE_DATE:
		case HAWQ_TYPE_TIME:
		case HAWQ_TYPE_TIMETZ:
		case HAWQ_TYPE_TIMESTAMP:
		case HAWQ_TYPE_TIMESTAMPTZ:
		case HAWQ_TYPE_INTERVAL:
		/* other types */
		case HAWQ_TYPE_MACADDR:
		case HAWQ_TYPE_INET:
		case HAWQ_TYPE_CIDR:
		case HAWQ_TYPE_BIT:
		case HAWQ_TYPE_VARBIT:
			initPrimitiveType(field,
							  NameStr(att->attname), /* field name*/
							  att->attnotnull ? REQUIRED : OPTIONAL, /* repetition */
							  att->atttypid,	/* HAWQ type */
							  att->attlen,		/* type len */
							  0,						/* r */
							  att->attnotnull ? 0 : 1,	/* d */
							  1,						/* depth */
							  NULL);	/* parent field path */
			colCount += 1;
			schemaTreeNodeCount += 1;
			break;
		/** embedded types */
		case HAWQ_TYPE_POINT:
			initPointType(field, att);
			colCount += 2;
			schemaTreeNodeCount += 3;
			break;
		case HAWQ_TYPE_PATH:
			initPathType(field, att);
			colCount += 3;
			schemaTreeNodeCount += 5;
			break;
		case HAWQ_TYPE_LSEG:
		case HAWQ_TYPE_BOX:
			initLsegBoxType(field, att);
			colCount += 4;
			schemaTreeNodeCount += 5;
			break;
		case HAWQ_TYPE_POLYGON:
			initPolygonType(field, att);
			colCount += 6;
			schemaTreeNodeCount += 9;
			break;
		case HAWQ_TYPE_CIRCLE:
			initCircleType(field, att);
			colCount += 3;
			schemaTreeNodeCount += 4;
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupport type '%s'", NameStr(att->attname))));
			break;
		}
	}
	parquetmd->colCount = colCount;
	parquetmd->schemaTreeNodeCount = schemaTreeNodeCount;

	parquetmd->hawqschemastr = generateHAWQSchemaStr(parquetmd->pfield,
													 parquetmd->fieldCount);
	parquetmd->maxBlockCount = DEFAULT_ROWGROUP_COUNT;
	parquetmd->pBlockMD = palloc0(parquetmd->maxBlockCount * sizeof(RowGroupMetadata));
	return 0;
}

/*
 * init primitive type
 */
void initPrimitiveType(struct FileField_4C *field,
					   char *name,
					   enum RepetitionType repetitionType,
					   int hawqType,
					   int typeLen,
					   int r,
					   int d,
					   int depth,
					   char *parentPathInSchema)
{
	int nameLen = strlen(name);
	int pathInSchemaLen;
	/*initialize name*/
	field->name = (char*) palloc0(nameLen + 1);
	strcpy(field->name, name);

	/*initialize pathInSchema, should be parentPathInSchema:Name*/
	if (parentPathInSchema == NULL) {
		pathInSchemaLen = nameLen;
		field->pathInSchema = (char*) palloc0(pathInSchemaLen + 1);
		strcpy(field->pathInSchema, name);
	} else {
		pathInSchemaLen = strlen(parentPathInSchema) + nameLen + 1;
		field->pathInSchema = (char*) palloc0(pathInSchemaLen + 1);
		strcpy(field->pathInSchema, parentPathInSchema);
		strcat(field->pathInSchema, ":");
		strcat(field->pathInSchema, name);
	}

	/*initialize other fields*/
	field->repetitionType = repetitionType;
	field->type = mappingHAWQType(hawqType);
	field->hawqTypeId = hawqType;
	field->typeLength = typeLen;
	field->r = r;
	field->d = d;
	field->depth = depth;
}

/**
 * initialize group type
 */
void initGroupType(struct FileField_4C *field,
				   char *name,
				   enum RepetitionType repetitionType,
				   int hawqType,
				   int r,
				   int d,
				   int depth,
				   int numChildren,
				   char *parentPathInSchema)
{
	int nameLen = strlen(name);
	int pathInSchemaLen;
	/*initialize name*/
	field->name = (char*) palloc0(nameLen + 1);
	strcpy(field->name, name);

	/*initialize pathInSchema, should be parentPathInSchema:Name*/
	if (parentPathInSchema == NULL) {
		pathInSchemaLen = nameLen;
		field->pathInSchema = (char*) palloc0(pathInSchemaLen + 1);
		strcpy(field->pathInSchema, name);
	} else {
		pathInSchemaLen = strlen(parentPathInSchema) + nameLen + 1;
		field->pathInSchema = (char*) palloc0(pathInSchemaLen + 1);
		strcpy(field->pathInSchema, parentPathInSchema);
		strcat(field->pathInSchema, ":");
		strcat(field->pathInSchema, name);
	}

	/*initialize other fields*/
	field->repetitionType = repetitionType;
	field->hawqTypeId = hawqType;
	field->r = r;
	field->d = d;
	field->depth = depth;
	field->num_children = numChildren;
	field->children =
			(struct FileField_4C*) palloc0 (sizeof(struct FileField_4C) * field->num_children);
}

/**
 * initPointType
 * point {required double x; required double y}
 */
void initPointType(struct FileField_4C *pointField, Form_pg_attribute att) {
	/*point itself*/
	enum RepetitionType repetitonType = att->attnotnull ? REQUIRED : OPTIONAL;
	int r = 0;
	int d = att->attnotnull ? 0 : 1;
	int depth = 1;
	int numChildren = 2;
	char *parentPathInSchema = NULL;

	initGroupType(pointField, NameStr(att->attname), repetitonType, att->atttypid,
				  r, d, depth,
				  numChildren, parentPathInSchema);

	/*point:x*/
	struct FileField_4C *child_0 = &(pointField->children[0]);
	initPrimitiveType(child_0, "x", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  pointField->r, pointField->d, pointField->depth + 1,
					  pointField->pathInSchema);

	/*point:y*/
	struct FileField_4C *child_1 = &(pointField->children[1]);
	initPrimitiveType(child_1, "y", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  pointField->r, pointField->d, pointField->depth + 1,
					  pointField->pathInSchema);
}

/**
 * initPathType
 * path:	group {	required boolean is_open;
 * 					repeated group points {required double x; required double y;}}
 */
void initPathType(struct FileField_4C* pathField, Form_pg_attribute att) {
	/*path itself*/
	enum RepetitionType repetitionType = att->attnotnull ? REQUIRED : OPTIONAL;
	int r = 0;
	int d = att->attnotnull ? 0 : 1;
	int depth = 1;
	int numChildren = 2;
	char *parentPathInSchema = NULL;
	initGroupType(pathField, NameStr(att->attname), repetitionType, att->atttypid,
				  r, d, depth,
				  numChildren, parentPathInSchema);

	/* path:is_open */
	struct FileField_4C *child_0 = &(pathField->children[0]);
	initPrimitiveType(child_0, "is_open", REQUIRED, HAWQ_TYPE_BOOL, /*FIXME is bool typeLen 1?*/1,
					  pathField->r, pathField->d, pathField->depth + 1,
					  pathField->pathInSchema);

	/* path:points */
	struct FileField_4C *child_1 = &(pathField->children[1]);
	initGroupType(child_1, "points", REPEATED, HAWQ_TYPE_POINT,
				  pathField->r + 1, pathField->d + 1, pathField->depth + 1,
				  2/*numChildren*/, pathField->pathInSchema);

	/* path:points:x */
	struct FileField_4C * child_1_0 = &(child_1->children[0]);
	initPrimitiveType(child_1_0, "x", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  child_1->r, child_1->d, child_1->depth + 1,
					  child_1->pathInSchema);

	/* path:points:y */
	struct FileField_4C * child_1_1 = &(child_1->children[1]);
	initPrimitiveType(child_1_1, "y", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  child_1->r, child_1->d, child_1->depth + 1,
					  child_1->pathInSchema);
}

/**
 * initLsegBoxType
 * lseg(601)			group {required double x1; required double y1; required double x2; required double y2;}
 *
 * box(603)				group {required double x1; required double y1; required double x2; required double y2;}
 *
 */
void initLsegBoxType(struct FileField_4C *lsegBoxField, Form_pg_attribute att) {
	/*lseg/box itself*/
	enum RepetitionType repetitionType = att->attnotnull ? REQUIRED : OPTIONAL;
	int r = 0;
	int d = att->attnotnull ? 0 : 1;
	int depth = 1;
	int numChildren = 4;
	char *parentPathInSchema = NULL;
	initGroupType(lsegBoxField, NameStr(att->attname), repetitionType, att->atttypid,
				  r, d, depth,
				  numChildren, parentPathInSchema);

	struct FileField_4C *child_0 = &(lsegBoxField->children[0]);
	initPrimitiveType(child_0, "x1", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  lsegBoxField->r, lsegBoxField->d, lsegBoxField->depth + 1,
					  lsegBoxField->pathInSchema);

	struct FileField_4C *child_1 = &(lsegBoxField->children[1]);
	initPrimitiveType(child_1, "y1", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  lsegBoxField->r, lsegBoxField->d, lsegBoxField->depth + 1,
					  lsegBoxField->pathInSchema);

	struct FileField_4C *child_2 = &(lsegBoxField->children[2]);
	initPrimitiveType(child_2, "x2", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  lsegBoxField->r, lsegBoxField->d, lsegBoxField->depth + 1,
					  lsegBoxField->pathInSchema);

	struct FileField_4C *child_3 = &(lsegBoxField->children[3]);
	initPrimitiveType(child_3, "y2", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  lsegBoxField->r, lsegBoxField->d, lsegBoxField->depth + 1,
					  lsegBoxField->pathInSchema);
}

/**
 * init polygon type.
 * group {
 *     required group boundbox {
 *         required double x1;
 *         required double y1;
 *         required double x2; 
 *         required double y2;
 *     },
 *     repeated group points {
 *         required double x;
 *         required double y;
 *     }
 * }
 */ 
void initPolygonType(FileField_4C *polygonField,Form_pg_attribute att)
{
	FileField_4C *box, *points, *f;
	RepetitionType repetitionType = att->attnotnull ? REQUIRED : OPTIONAL;
	int r = 0;
	int d = att->attnotnull ? 0 : 1;
	int depth = 1;
	int numChildren = 2;
	char *parentPathInSchema = NULL;

	/* polygon is a group */
	initGroupType(polygonField, NameStr(att->attname), repetitionType, att->atttypid,
				  r, d, depth,
				  numChildren, parentPathInSchema);
	box = polygonField->children;
	points = polygonField->children + 1;

	/* polygon:boundbox */
	initGroupType(box, "boundbox", REQUIRED, HAWQ_TYPE_BOX,
				  polygonField->r, polygonField->d, polygonField->depth + 1,
				  4 /* numchild */, polygonField->pathInSchema);
	/* polygon:points */
	initGroupType(points, "points", REPEATED, HAWQ_TYPE_POINT,
				  polygonField->r + 1, polygonField->d + 1, polygonField->depth + 1,
				  2 /* numchild */, polygonField->pathInSchema);

	/* polygon:boundbox:{x1,y1,x2,y2} */
	f = box->children;
	initPrimitiveType(f, "x1", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  box->r, box->d, box->depth + 1,
					  box->pathInSchema);
	f = box->children + 1;
	initPrimitiveType(f, "y1", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  box->r, box->d, box->depth + 1,
					  box->pathInSchema);
	f = box->children + 2;
	initPrimitiveType(f, "x2", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  box->r, box->d, box->depth + 1,
					  box->pathInSchema);
	f = box->children + 3;
	initPrimitiveType(f, "y2", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  box->r, box->d, box->depth + 1,
					  box->pathInSchema);

	/* polygon:points:{x,y} */
	f = points->children;
	initPrimitiveType(f, "x", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  points->r, points->d, points->depth + 1,
					  points->pathInSchema);
	f = points->children + 1;
	initPrimitiveType(f, "y", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  points->r, points->d, points->depth + 1,
					  points->pathInSchema);
}

/**
 * initCircleType
 *
 * circle(718)			group {required double x; required double y; required double r;}
 *
 */
void initCircleType(struct FileField_4C *circleField, Form_pg_attribute att) {
	/*circle itself*/
	enum RepetitionType repetitionType = att->attnotnull ? REQUIRED : OPTIONAL;
	int r = 0;
	int d = att->attnotnull ? 0 : 1;
	int depth = 1;
	int numChildren = 3;
	char *parentPathInSchema = NULL;
	initGroupType(circleField, NameStr(att->attname), repetitionType, att->atttypid,
				  r, d, depth, numChildren, parentPathInSchema);

	struct FileField_4C *child_0 = &(circleField->children[0]);
	initPrimitiveType(child_0, "x", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  circleField->r, circleField->d, circleField->depth + 1,
					  circleField->pathInSchema);

	struct FileField_4C *child_1 = &(circleField->children[1]);
	initPrimitiveType(child_1, "y", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  circleField->r, circleField->d, circleField->depth + 1,
					  circleField->pathInSchema);

	struct FileField_4C *child_2 = &(circleField->children[2]);
	initPrimitiveType(child_2, "r", REQUIRED, HAWQ_TYPE_FLOAT8, 8,
					  circleField->r, circleField->d, circleField->depth + 1,
					  circleField->pathInSchema);
}

int
mappingHAWQType(int hawqTypeID)
{
	switch (hawqTypeID)
	{
	case HAWQ_TYPE_BOOL:
		return BOOLEAN;

	case HAWQ_TYPE_INT2:
	case HAWQ_TYPE_INT4:
	case HAWQ_TYPE_MONEY:
	case HAWQ_TYPE_DATE:
		return INT32;

	case HAWQ_TYPE_INT8:
	case HAWQ_TYPE_TIME:
	case HAWQ_TYPE_TIMESTAMPTZ:
	case HAWQ_TYPE_TIMESTAMP:
		return INT64;

	case HAWQ_TYPE_FLOAT4:
		return FLOAT;

	case HAWQ_TYPE_FLOAT8:
		return DOUBLE;

	case HAWQ_TYPE_BIT:
	case HAWQ_TYPE_VARBIT:
	case HAWQ_TYPE_BYTE:
	case HAWQ_TYPE_NUMERIC:
	case HAWQ_TYPE_NAME:
	case HAWQ_TYPE_CHAR:
	case HAWQ_TYPE_BPCHAR:
	case HAWQ_TYPE_VARCHAR:
	case HAWQ_TYPE_TEXT:
	case HAWQ_TYPE_XML:
	case HAWQ_TYPE_TIMETZ:
	case HAWQ_TYPE_INTERVAL:
	case HAWQ_TYPE_MACADDR:
	case HAWQ_TYPE_INET:
	case HAWQ_TYPE_CIDR:
		return BINARY;

	default:
		Insist(false);
		return -1;
	}
}

ParquetRowGroup
addRowGroup(ParquetMetadata parquetmd,
			AppendOnlyEntry *aoentry,
			File file)
{
	ParquetRowGroup rowgroup = palloc0(sizeof(struct ParquetRowGroup_S));

	/* ParquetRowGroup */
	rowgroup->catalog			= aoentry;
	rowgroup->rowGroupMetadata	= palloc0(sizeof(struct BlockMetadata_4C));
	rowgroup->columnChunkNumber	= parquetmd->colCount;
	rowgroup->columnChunks		= palloc0(parquetmd->colCount * sizeof(struct ParquetColumnChunk_S));
	rowgroup->parquetFile		= file;

	/* RowGroupMetadata */
	RowGroupMetadata rowgroupmd = rowgroup->rowGroupMetadata;
	rowgroupmd->ColChunkCount	= parquetmd->colCount;
	rowgroupmd->columns			= palloc0(parquetmd->colCount * sizeof(struct ColumnChunkMetadata_4C));
	rowgroupmd->rowCount		= 0;
	rowgroupmd->totalByteSize	= 0;

	/*
	 * init ParquetColumnChunk (including it's metadata) for each column
	 */
	int cIndex = 0;
	for (int i = 0; i < parquetmd->fieldCount; ++i)
	{
		addSingleColumn(rowgroup->catalog,
						&rowgroupmd->columns,	/* for ColumnChunkMetadata */
						rowgroup->columnChunks,	/* for ParquetColumnChunk */
						rowgroup->catalog->blocksize / rowgroup->columnChunkNumber,
						/*parquet column chunk max size*/
						&parquetmd->pfield[i],
						&cIndex,
						rowgroup->parquetFile);
	}
	Assert(cIndex == parquetmd->colCount);

	/* ParquetMetadata should keep track of all rowgroups' metadata */
	if (parquetmd->blockCount >= parquetmd->maxBlockCount)
	{
		parquetmd->maxBlockCount *= 2;
		parquetmd->pBlockMD = repalloc(parquetmd->pBlockMD,
									   parquetmd->maxBlockCount * sizeof(RowGroupMetadata));
	}
	parquetmd->pBlockMD[parquetmd->blockCount++] = rowgroupmd;

	return rowgroup;
}

void
flushRowGroup(ParquetRowGroup rowgroup,
			  ParquetMetadata parquetmd,
			  MirroredAppendOnlyOpen *mirroredOpen,
			  int64 *fileLen,
			  int64 *fileLen_uncompressed)
{
	int bytes_added = 0;

	if (rowgroup == NULL)
		return;

	/*
	 * Write out column chunks one by one. For each chunk, we do the following:
	 * 1. encode the last page.
	 * 2. write out pages one by one.
	 * 3. write out chunk's metadata after the last page.
	 */
	for (int i = 0; i < rowgroup->columnChunkNumber; i++)
	{
		ParquetColumnChunk chunk	= &rowgroup->columnChunks[i];
		ColumnChunkMetadata chunkmd	= chunk->columnChunkMetadata;

		bytes_added += encodeCurrentPage(chunk);

		/*----------------------------------------------------------------
		 * write out pages one by one
		 *----------------------------------------------------------------*/
		chunkmd->firstDataPage = FileNonVirtualTell(rowgroup->parquetFile);
		if (chunkmd->firstDataPage < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("file tell position error for segment file: %s",
							 strerror(errno))));
		}
		for (int pageno = 0; pageno < chunk->pageNumber; ++pageno)
		{
			flushDataPage(chunk, pageno);
		}

		/*----------------------------------------------------------------
		 * write out chunk's metadata after the last page
		 *----------------------------------------------------------------*/
		chunkmd->file_offset = FileNonVirtualTell(rowgroup->parquetFile);
		if (chunkmd->file_offset < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("file tell position error for segment file: %s",
							 strerror(errno))));
		}

		uint8_t *Thrift_ColumnMetaData_Buf;
		uint32_t Thrift_ColumnMetaData_Len;
		if (writeColumnChunkMetadata(&Thrift_ColumnMetaData_Buf,
									 &Thrift_ColumnMetaData_Len,
									 chunkmd) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR),
					 errmsg("failed to serialize column metadata using thrift")));
		}

		bytes_added += Thrift_ColumnMetaData_Len;

		if (FileWrite(rowgroup->parquetFile,
					  (char *) Thrift_ColumnMetaData_Buf,
					  Thrift_ColumnMetaData_Len) != Thrift_ColumnMetaData_Len)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("file write error when writing out column metadata: %s",
							 strerror(errno))));
		}

		/* Add chunk compressedsize and uncompressedsize to parquet fileLen and fileLen_uncompressed*/
		(*fileLen) += (chunkmd->totalSize + Thrift_ColumnMetaData_Len);
		(*fileLen_uncompressed) += (chunkmd->totalUncompressedSize + Thrift_ColumnMetaData_Len);
	}

	int fileSync = 0;
	MirroredAppendOnly_Flush(mirroredOpen, &fileSync);
	if(fileSync < 0){
		ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("file sync error: %s",
							 strerror(fileSync))));
	}

	rowgroup->rowGroupMetadata->totalByteSize += bytes_added;
	parquetmd->num_rows += rowgroup->rowGroupMetadata->rowCount;

	freeRowGroup(rowgroup);
}

void
freeRowGroup(ParquetRowGroup rowgroup)
{
	for (int i = 0; i < rowgroup->columnChunkNumber; i++)
	{
		pfree(rowgroup->columnChunks[i].pages);
		if (rowgroup->columnChunks[i].compressed != NULL)
		{
			pfree(rowgroup->columnChunks[i].compressed);
		}

		/* chunk metadata should be kept util parquet_insert_finish */
		rowgroup->columnChunks[i].columnChunkMetadata = NULL;
	}
	pfree(rowgroup->columnChunks);
	pfree(rowgroup);
}

/**
 * add a column information to row group
 * columnsMetadata:		columnMetadata needed to be added
 * columns:				column chunks of row group needed to be initialized
 * maxChunkLimitSize:	the max size of the column chunk in memory, calcualted by rowgroupsize/columnNum
 * field:				the column description in parquet file metadata schema part
 * colIndex:			the index of the column in columnsMetadata and columns
 */
void
addSingleColumn(AppendOnlyEntry *catalog,
				struct ColumnChunkMetadata_4C** columnsMetadata,
				ParquetColumnChunk columns,
				int maxChunkLimitSize,
				struct FileField_4C *field,
				int *colIndex,
				File parquetFile)
{
	if (field->num_children > 0) {
		/* for embedded types, should expand it, recursive call the function itself*/
		for (int i = 0; i < field->num_children; i++) {
			addSingleColumn(catalog, columnsMetadata, columns, maxChunkLimitSize, &(field->children[i]),
					colIndex, parquetFile);
		}
	} else {/* for single column, directly add the column*/
		struct ColumnChunkMetadata_4C* columnChunkMetadata =
				&((*columnsMetadata)[*colIndex]);
		/*maybe 3?? for definition level, repetition level, and data*/
		columnChunkMetadata->EncodingCount = 3;
		columnChunkMetadata->pEncodings =
				(enum Encoding*) palloc0(columnChunkMetadata->EncodingCount * sizeof(enum Encoding));
		columnChunkMetadata->pEncodings[0] = RLE; /*set definition level encoding as RLE*/
		columnChunkMetadata->pEncodings[1] = RLE; /*set repetition level encoding as RLE*/
		columnChunkMetadata->pEncodings[2] = PLAIN; /*set data encoding as PLAIN*/
		columnChunkMetadata->file_offset = 0;
		columnChunkMetadata->firstDataPage = 0;
		columnChunkMetadata->totalSize = 0;
		columnChunkMetadata->totalUncompressedSize = 0;
		columnChunkMetadata->valueCount = 0;

		if (catalog->compresstype == NULL)
		{
			columnChunkMetadata->codec = UNCOMPRESSED;
		}
		else
		{
			if (0 == strcmp(catalog->compresstype, "snappy")){
				columnChunkMetadata->codec = SNAPPY;
			}
			else if (0 == strcmp(catalog->compresstype, "gzip")){
				columnChunkMetadata->codec = GZIP;
			}
			else if (0 == strcmp(catalog->compresstype, "lzo")){
				columnChunkMetadata->codec = LZO;
			}
			else {
				Assert(0 == strcmp(catalog->compresstype, "none"));
				columnChunkMetadata->codec = UNCOMPRESSED;	
			}
		}
		
		/* stores */
		columnChunkMetadata->type = field->type;
		columnChunkMetadata->hawqTypeId = field->hawqTypeId;
		int attNameLen = strlen(field->name);
		columnChunkMetadata->colName = (char*) palloc0(attNameLen + 1);
		strcpy(columnChunkMetadata->colName, field->name);
		int pathInSchemaLen = strlen(field->pathInSchema);
		columnChunkMetadata->pathInSchema =
				(char*) palloc0(pathInSchemaLen + 1);
		strcpy(columnChunkMetadata->pathInSchema, field->pathInSchema);
		columnChunkMetadata->r = field->r;
		columnChunkMetadata->d = field->d;
		columnChunkMetadata->depth = field->depth;

		/** actual data for column chunks part*/
		/* may also need copy data here*/
		ParquetColumnChunk chunk = columns + (*colIndex);
		chunk->columnChunkMetadata = columnChunkMetadata;
		chunk->pageNumber = 0;
		chunk->maxPageCount = DEFAULT_DATAPAGE_COUNT;
		chunk->pages = (ParquetDataPage) palloc0(chunk->maxPageCount * sizeof(struct ParquetDataPage_S));
		chunk->currentPage		= NULL;
		chunk->parquetFile		= parquetFile;
		chunk->maxPageLimitSize = catalog->pagesize;

		/* set maxPagesize to min(chunk->maxPageLimitSize, maxChunkLimitSize) */
		chunk->maxPageSize = (chunk->maxPageLimitSize > maxChunkLimitSize) ?
				maxChunkLimitSize : chunk->maxPageLimitSize;

		chunk->compresstype		= catalog->compresstype;
		chunk->compresslevel	= catalog->compresslevel;
		
		if (columnChunkMetadata->codec != UNCOMPRESSED)
		{
			/* 1.5 shoud be an empirical value to make our buffer big enough for most page */
			chunk->compressedMaxLen	= chunk->maxPageSize * 1.5;
			chunk->compressed = (char *) palloc0(chunk->compressedMaxLen);
		}

		*colIndex = *colIndex + 1;
	}
}

/*
 * Write out a specified data page (page header + page data)
 */
static void
flushDataPage(ParquetColumnChunk chunk, int page_number)
{
	ParquetDataPage page = &chunk->pages[page_number];
	Assert(page != NULL);
	Assert(page->finalized);
	Assert(page->header_buffer != NULL);

	/*----------------------------------------------------------------
	 * write out thrift page header
	 *----------------------------------------------------------------*/
	if (FileWrite(page->parquetFile,
				  (char *) page->header_buffer,
				  page->header_len) != page->header_len)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("file write error when writing out page header: %s",
						strerror(errno))));
	}
	pfree(page->header_buffer);

	/*----------------------------------------------------------------
	 * write out page data
	 *----------------------------------------------------------------*/
	if (FileWrite(page->parquetFile,
				  (char *) page->data,
				  page->header->compressed_page_size) != page->header->compressed_page_size)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("file write error when writing out page data: %s",
						strerror(errno))));
	}

	pfree(page->header);
	if (page->data != NULL)
	{
		pfree(page->data);
	}
}

/*
 * Append value to the corresponding data page.
 * 
 * Upon successful completion, the number of bytes which were added is returned.
 *
 * Otherwise ENCODE_INVALID_VALUE is returned if encoded length of `data`
 * exceeds `maxPageSize`.
 *
 * ENCODE_OUTOF_PAGE is returned if `data` is of valid size but appending
 * the data will make the page exceeds `maxPageSize`.
 *
 * @maxPageSize:		min(maxPageLimitSize, rowgoupLimitSize/columnNum)
 * @maxPageLimitSize:	the page size limited by user or default pagesize 1MB/4MB
 *
 * If the encodePlain exceeds maxPageSize but not exceeds maxPageLimitSize, should
 * repalloc dataPage->values_buffer to enable a large single value limitation
 * can satisfy user's expection, which means, user defines the page limit to 3MB, rowgroup
 * limit to 10MB with 5 columns, when one value exceeds 2MB but not 3MB, should enable
 * this value rather than throw exception. [JIRA: GPSQL-1500]
 *
 */
static int
encodePlain(Datum data, 
			ParquetDataPage current_page,
			int hawqTypeId,
			int maxPageSize,
			int maxPageLimitSize)
{
	int len = 0; /* actual number of bytes added to buffer */
	uint8_t* dst_ptr = NULL;

	switch (hawqTypeId)
	{

	case HAWQ_TYPE_BOOL:
	{
		/* If the size exceeds current_page size, should return OUTOF_PAGE*/
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page),
				maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}
		return BitPack_WriteInt(current_page->bool_values, DatumGetBool(data) ? 1 : 0);
	}

	/*----------------------------------------------------------------
	 * Type mapped to 4-bytes INT32/FLOAT in Parquet
	 *----------------------------------------------------------------*/
	case HAWQ_TYPE_INT2:
	{
		len = 4;
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
				maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}
		int32 val = (int32) DatumGetInt16(data);

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size; 

		memcpy(dst_ptr, &val, len);
		return len;
	}
	case HAWQ_TYPE_INT4:
	{
		len = 4;
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
				maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}
		int32 val = DatumGetInt32(data);

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size;

		memcpy(dst_ptr, &val, len);
		return len;
	}
	case HAWQ_TYPE_DATE:
	{
		len = 4;
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
				maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}
		DateADT val = DatumGetDateADT(data);

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size;

		memcpy(dst_ptr, &val, len);
		return len;
	}
	case HAWQ_TYPE_FLOAT4:
	{
		len = 4;
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
			maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}
		float4 val = DatumGetFloat4(data);

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size;

		memcpy(dst_ptr, &val, len);
		return len;
	}
	case HAWQ_TYPE_MONEY:
	{
		/*
		 * Although money is represented as int32 internally,
		 * it's passed by reference.
		 */
		len = 4;
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
			maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}
		Cash *cash_p = (Cash *) DatumGetPointer(data);

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size;

		memcpy(dst_ptr, cash_p, len);
		return len;
	}


	/*----------------------------------------------------------------
	 * Type mapped to 8-bytes INT64/DOUBLE in Parquet
	 *----------------------------------------------------------------*/
	case HAWQ_TYPE_INT8:
	{
		len = 8;
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
			maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}
		int64 val = DatumGetInt64(data);

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size;

		memcpy(dst_ptr, &val, len);
		return len;
	}
	case HAWQ_TYPE_TIME:
	{
		len = 8;
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
			maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}
		TimeADT val = DatumGetTimeADT(data);

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size;

		memcpy(dst_ptr, &val, len);
		return len;
	}
	case HAWQ_TYPE_TIMESTAMP:
	{
		len = 8;
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
			maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}
		Timestamp val = DatumGetTimestamp(data);

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size;

		memcpy(dst_ptr, &val, len);
		return len;
	}
	case HAWQ_TYPE_TIMESTAMPTZ:
	{
		len = 8;
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
			maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}
		TimestampTz val = DatumGetTimestampTz(data);

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size;

		memcpy(dst_ptr, &val, len);
		return len;
	}
	case HAWQ_TYPE_FLOAT8:
	{
		len = 8;
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
			maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}
		float8 val = DatumGetFloat8(data);

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size;

		memcpy(dst_ptr, &val, len);
		return len;
	}

	/*----------------------------------------------------------------
	 * fixed length type, mapped to BINARY in Parquet
	 *----------------------------------------------------------------*/
	case HAWQ_TYPE_NAME:
	{
		int data_size = NAMEDATALEN;
		NameData *name = DatumGetName(data);
		len = 4 + data_size;
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
			maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size;

		memcpy(dst_ptr, &(/*htole32(*/data_size/*)*/), 4);
		dst_ptr += 4;
		memcpy(dst_ptr, NameStr(*name), data_size);
		return len;
	}
	case HAWQ_TYPE_TIMETZ: 
	{
		/*
		 * timetz (12 bytes) is stored in parquet's BINARY type,
		 * that is <4-bytes-header> + <12-bytes-content>
		 */
		int data_size = sizeof(TimeTzADT);
		TimeTzADT *timetz = DatumGetTimeTzADTP(data);
		len = 4 + data_size;
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
			maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size;

		memcpy(dst_ptr, &(/*htole32(*/data_size/*)*/), 4);
		dst_ptr += 4;
		memcpy(dst_ptr, timetz, sizeof(TimeTzADT));
		return len;
	}
	case HAWQ_TYPE_INTERVAL:
	{
		int data_size = sizeof(Interval);
		Interval *interval = DatumGetIntervalP(data);
		len = 4 + data_size;
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
			maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size;

		memcpy(dst_ptr, &(/*htole32(*/data_size/*)*/), 4);
		dst_ptr += 4;
		memcpy(dst_ptr, interval, sizeof(Interval));
		return len;
	}
	case HAWQ_TYPE_MACADDR:
	{
		int data_size = 6;
		macaddr *mac = DatumGetMacaddrP(data);
		len = 4 + data_size;
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
			maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size;

		memcpy(dst_ptr, &(/*htole32(*/data_size/*)*/), 4);
		dst_ptr += 4;
		/* TODO can we just memcpy the structure? */
		memcpy(dst_ptr, &mac->a, sizeof(char));
		dst_ptr += sizeof(char);
		memcpy(dst_ptr, &mac->b, sizeof(char));
		dst_ptr += sizeof(char);
		memcpy(dst_ptr, &mac->c, sizeof(char));
		dst_ptr += sizeof(char);
		memcpy(dst_ptr, &mac->d, sizeof(char));
		dst_ptr += sizeof(char);
		memcpy(dst_ptr, &mac->e, sizeof(char));
		dst_ptr += sizeof(char);
		memcpy(dst_ptr, &mac->f, sizeof(char));
		return len;
	}

	/*
	 * variable length type, mapped to BINARY in Parquet
	 * ------------------
	 * The following types are implemented as varlena in HAWQ, they all corresponds
	 * to BINARY in Parquet. However, we have two strategies when storing them, depends
	 * on whether we stores varlena header or not.
	 * 
	 * [strategy 1] exclude varlena header:
	 * 				BINARY = [VARSIZE(d) - 4, VARDATA(d)]
	 * 				This strategy works better for text-related type because
	 * 				any parquet client can interprete text binary.
	 *
	 * [strategy 2] include varlena header in actual data:
	 * 				BINARY = [VARSIZE(d), d]
	 * 				This works better for HAWQ specific type like numeric because
	 * 				we can easily deserialize data. (just get the data part of the byte array)
	 */
	
	/* these types use [strategy 1] */
	case HAWQ_TYPE_BYTE:
	case HAWQ_TYPE_CHAR:
	case HAWQ_TYPE_BPCHAR:
	case HAWQ_TYPE_VARCHAR:
	case HAWQ_TYPE_TEXT:
	case HAWQ_TYPE_XML:
	{
		struct varlena *varlen = (struct varlena *) DatumGetPointer(data);
		Assert(!VARATT_IS_COMPRESSED(varlen) && !VARATT_IS_EXTERNAL(varlen));

		int puredataSize = VARSIZE_ANY_EXHDR(varlen);
		len = puredataSize + 4;

		if (len > maxPageLimitSize)
		{
			return ENCODE_INVALID_VALUE;
		}
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
			maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size;

		memcpy(dst_ptr, &(/*htole32(*/puredataSize/*)*/), 4);
		dst_ptr += 4;
		memcpy(dst_ptr, VARDATA_ANY(varlen), puredataSize);
		return len;
	}
	/* these types use [strategy 2] */
	case HAWQ_TYPE_BIT:
	case HAWQ_TYPE_VARBIT:
	case HAWQ_TYPE_NUMERIC:
	case HAWQ_TYPE_INET:
	case HAWQ_TYPE_CIDR:
	{
		struct varlena *varlen = (struct varlena *) DatumGetPointer(data);
		Assert(!VARATT_IS_COMPRESSED(varlen) && !VARATT_IS_EXTERNAL(varlen));

		int dataSize = VARSIZE_ANY(varlen);
		len = dataSize + sizeof(int32);

		if (len > maxPageLimitSize)
		{
			return ENCODE_INVALID_VALUE;
		}
		if (exceedsPageSizeLimit(current_page, approximatePageSize(current_page) + len,
			maxPageSize, maxPageLimitSize))
		{
			return ENCODE_OUTOF_PAGE;
		}

                /* exceedsPageSizeLimit may change the address of 
                   current_page->values_buffer. */
                dst_ptr = current_page->values_buffer + 
                          current_page->header->uncompressed_page_size;

		memcpy(dst_ptr, &(/*htole32(*/dataSize/*)*/), sizeof(int32));
		dst_ptr += sizeof(int32);
		memcpy(dst_ptr, varlen, dataSize);
		return len;
	}
	default:
		Insist(false);
		break;
	}
}

/**
 * Judge whether current_page size exceeds Limitation
 *
 * If the current page size doesn't exceed maxPageSize, return false;
 * If exceed maxPageSize but not exceeds maxPageLimitSize, repalloc
 * current_page->values_buffer;
 * Otherwise, return true
 */
static bool exceedsPageSizeLimit(ParquetDataPage current_page,
		int currentPageSize, int maxPageSize, int maxPageLimitSize)
{
	Assert(maxPageSize <= maxPageLimitSize);

	if (currentPageSize >= maxPageLimitSize)
		return true;

	if (currentPageSize > maxPageSize)
		current_page->values_buffer = repalloc(current_page->values_buffer, maxPageLimitSize);

	return false;
}

/**
 * Put the final page data (may be compressed) in page->data, and the
 * final page header data in page->header_buffer.
 *
 * Return added size of uncompressed data in the current row group, which
 * is 'unflushed rle/bitpack data' + 'page header'
 */
int
encodeCurrentPage(ParquetColumnChunk chunk)
{
	int bytes_added;
	ParquetDataPage current_page;
	ParquetPageHeader header;
	ColumnChunkMetadata chunkmd;

	bytes_added = 0;
	current_page = chunk->currentPage;
	header = current_page->header;
	chunkmd = chunk->columnChunkMetadata;

	Assert(current_page != NULL);

	if (current_page->finalized)
		return 0;

	/*----------------------------------------------------------------
	 * Flush RLE/BitPack encoded data. Size of r and d data are
	 * accumulated into page's uncompressed_page_size in this phase.
	 * 
	 * r/d = <4-bytes little-endian encoded-data-len> + <encoded-data>
	 *----------------------------------------------------------------*/
	if(current_page->repetition_level != NULL)
	{
		RLEEncoder_Flush(current_page->repetition_level);
		bytes_added += 4 + RLEEncoder_Size(current_page->repetition_level);
	}

	if(current_page->definition_level != NULL)
	{
		RLEEncoder_Flush(current_page->definition_level);
		bytes_added += 4 + RLEEncoder_Size(current_page->definition_level);
	}

	if (chunkmd->hawqTypeId == HAWQ_TYPE_BOOL)
	{
		bytes_added += BitPack_Flush(current_page->bool_values);
	}

	header->uncompressed_page_size += bytes_added;

	/* we must make sure there is no empty page, since some compression algorithm
	 * will fail if input buffer is NULL */
	Assert(header->uncompressed_page_size > 0);

	/*----------------------------------------------------------------
	 * Combine r/d/value bytes into a buffer for compressing.
	 *----------------------------------------------------------------*/
	StringInfoData buf;
	/* we don't want to StringInfo to enlarge its buffer during appendXXX,
	 * however StringInfo has a trailing '\0', so we add 1 here */
	initStringInfoOfSize(&buf, header->uncompressed_page_size + 1);

	if(current_page->repetition_level != NULL)
	{
		int encoded_data_len = RLEEncoder_Size(current_page->repetition_level);
		appendBinaryStringInfo(&buf, &(/*htole32(*/encoded_data_len/*)*/), 4);
		appendBinaryStringInfo(&buf,
							   RLEEncoder_Data(current_page->repetition_level),
							   encoded_data_len);

		pfree(current_page->repetition_level->writer.buffer);
		pfree(current_page->repetition_level->packBuffer);
		pfree(current_page->repetition_level);
	}

	if(current_page->definition_level != NULL)
	{
		int encoded_data_len = RLEEncoder_Size(current_page->definition_level);
		appendBinaryStringInfo(&buf, &(/*htole32(*/encoded_data_len/*)*/), 4);
		appendBinaryStringInfo(&buf,
							   RLEEncoder_Data(current_page->definition_level),
							   encoded_data_len);

		pfree(current_page->definition_level->writer.buffer);
		pfree(current_page->definition_level->packBuffer);
		pfree(current_page->definition_level);
	}

	if (chunkmd->hawqTypeId == HAWQ_TYPE_BOOL)
	{
		appendBinaryStringInfo(&buf,
							   BitPack_Data(current_page->bool_values),
							   BitPack_Size(current_page->bool_values));

		pfree(current_page->bool_values);
	}
	else
	{
		appendBinaryStringInfo(&buf,
							   current_page->values_buffer,
							   header->uncompressed_page_size - buf.len);

		pfree(current_page->values_buffer);
	}

	/*----------------------------------------------------------------
	 * Compress page data if needed, saved it to current_page->data.
	 *----------------------------------------------------------------*/
	switch (chunkmd->codec)
	{
		case UNCOMPRESSED:
		{
			current_page->data = (uint8_t*) buf.data;
			header->compressed_page_size = header->uncompressed_page_size;
			break;
		}

		case SNAPPY:
		{
			size_t compressedLen = snappy_max_compressed_length(header->uncompressed_page_size);

			if (compressedLen > chunk->compressedMaxLen)
			{
				chunk->compressedMaxLen = compressedLen;
				chunk->compressed = repalloc(chunk->compressed, chunk->compressedMaxLen);
			}

			if (snappy_compress(buf.data, header->uncompressed_page_size,
								chunk->compressed, &compressedLen) == SNAPPY_OK)
			{
				/* we don't need uncompressed data any more, reuse buf.data to store
				 * final compressed content */
				buf.data = repalloc(buf.data, compressedLen);
				memcpy(buf.data, chunk->compressed, compressedLen);

				current_page->data = (uint8_t *) buf.data;
				header->compressed_page_size = compressedLen;
			}
			else
			{	/* shouldn't get here */
				Insist(false);
			}

			break;
		}
		case GZIP:
		{
			int ret;
			/* 15(default windowBits for deflate) + 16(ouput GZIP header/tailer) */
			const int windowbits = 31;

			z_stream stream;
			stream.zalloc	= Z_NULL;
			stream.zfree	= Z_NULL;
			stream.opaque	= Z_NULL;
			stream.avail_in	= header->uncompressed_page_size;
			stream.next_in	= (Bytef *) buf.data;

			ret = deflateInit2(&stream, chunk->compresslevel, Z_DEFLATED,
							   windowbits, MAX_MEM_LEVEL, Z_DEFAULT_STRATEGY);
			if (ret != Z_OK)
			{
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERNAL_ERROR),
						 errmsg("zlib deflateInit2 failed: %s", stream.msg)));
			}

			Bytef *out = (Bytef *) chunk->compressed;
			int outlen = chunk->compressedMaxLen;
			
			/* process until all inputs have been compressed */
			do
			{
				stream.next_out = out;
				stream.avail_out = outlen;

				ret = deflate(&stream, Z_FINISH);
				if (ret == Z_STREAM_END)
					break;
				if (ret == Z_OK)	/* out buffer is not big enough */
				{
					chunk->compressedMaxLen += 4096;
					chunk->compressed = repalloc(chunk->compressed, chunk->compressedMaxLen);
					out += outlen;
					outlen = 4096;
				}
				else
				{
					deflateEnd(&stream);
					ereport(ERROR,
							(errcode(ERRCODE_GP_INTERNAL_ERROR),
							 errmsg("zlib deflate failed: %s", stream.msg)));
				}

			} while (1);

			outlen = stream.total_out;

			deflateEnd(&stream);

			/* copy the final compressed data */
			buf.data = repalloc(buf.data, outlen);
			memcpy(buf.data, chunk->compressed, outlen);
			
			current_page->data = (uint8_t *) buf.data;
			header->compressed_page_size = outlen;
			break;
		}
		case LZO:
			/* TODO*/
			Insist(false);
			break;
		default:
			Insist(false);	/* shouldn't get here */
			break;
	}

	/*----------------------------------------------------------------
	 * All fields of page header are filled, convert to binary page
	 * header in thrift.
	 *----------------------------------------------------------------*/
	uint8_t* header_buffer = NULL;
	if (writePageMetadata(&header_buffer, (uint32_t *) &current_page->header_len,
					  current_page->header) < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
				 errmsg("failed to serialize page metadata using thrift for column: %s", chunkmd->colName)));
	}

	current_page->header_buffer = (uint8_t *) palloc0(current_page->header_len);
	memcpy(current_page->header_buffer, header_buffer, current_page->header_len);

	chunkmd->totalUncompressedSize	+= current_page->header_len + header->uncompressed_page_size;
	chunkmd->totalSize				+= current_page->header_len + header->compressed_page_size;
	
	current_page->finalized = true;

	return bytes_added;
}

static void
addDataPage(ParquetColumnChunk chunk)
{
	if (chunk->pageNumber >= chunk->maxPageCount)
	{
		chunk->pages = repalloc(chunk->pages,
				2 * chunk->maxPageCount * sizeof(struct ParquetDataPage_S));
		/* make sure all allocated page memory are zero-filled */
		memset(chunk->pages + chunk->maxPageCount, 0, chunk->maxPageCount * sizeof(struct ParquetDataPage_S));
		chunk->maxPageCount *= 2;
	}

	chunk->currentPage = &chunk->pages[chunk->pageNumber];
	chunk->currentPage->data = NULL;
	chunk->currentPage->finalized = false;
	chunk->currentPage->header = (ParquetPageHeader) palloc0(sizeof(PageMetadata_4C));
	chunk->currentPage->header->page_type = DATA_PAGE;
	chunk->currentPage->header->definition_level_encoding = RLE;
	chunk->currentPage->header->repetition_level_encoding = RLE;
	chunk->currentPage->parquetFile = chunk->parquetFile;

	if (chunk->columnChunkMetadata->d != 0)
	{
		chunk->currentPage->definition_level = palloc0(sizeof(RLEEncoder));

		RLEEncoder_Init(chunk->currentPage->definition_level,
						widthFromMaxInt(chunk->columnChunkMetadata->d));
	}
	if (chunk->columnChunkMetadata->r != 0)
	{
		chunk->currentPage->repetition_level = palloc0(sizeof(RLEEncoder));

		RLEEncoder_Init(chunk->currentPage->repetition_level,
						widthFromMaxInt(chunk->columnChunkMetadata->r));
	}
	/* use BIT_PACK encoding for bool column */
	if (chunk->columnChunkMetadata->type == BOOLEAN)
	{
		chunk->currentPage->bool_values = palloc0(sizeof(ByteBasedBitPackingEncoder));
		BitPack_InitEncoder(chunk->currentPage->bool_values, /*bitWidth=*/1);
	}
	else
	{
		chunk->currentPage->values_buffer = palloc0(chunk->maxPageSize);
	}

	chunk->pageNumber++;
}

int
appendParquetColumnNull(ParquetColumnChunk columnChunk)
{
	int bytes_added = 0;

	/*if page is null, initialize a new page*/
	if ((columnChunk->pageNumber == 0) || (columnChunk->currentPage == NULL)) {
		addDataPage(columnChunk);
	}

	/* If page size exceeds limit, finalize current data page and add a new one*/
	if (approximatePageSize(columnChunk->currentPage) >= columnChunk->maxPageSize)
	{
		bytes_added += finalizeCurrentAndNewPage(columnChunk);
	}

	Assert(columnChunk->currentPage->definition_level != NULL);
	RLEEncoder_WriteInt(columnChunk->currentPage->definition_level, 0);

	if (columnChunk->currentPage->repetition_level != NULL)
	{
		RLEEncoder_WriteInt(columnChunk->currentPage->repetition_level, 0);
	}

	columnChunk->currentPage->header->num_values++;
	columnChunk->columnChunkMetadata->valueCount++;
	return bytes_added;
}

/**
 * Finalize current data page, and then add a new page
 * @columnChunk:		The column chunk which needs to add data
 * @bytes_added:		The number of bytes added
 *
 * return uncompressed bytes added to current row group
 */
int
finalizeCurrentAndNewPage(ParquetColumnChunk columnChunk)
{
	int bytes_added = encodeCurrentPage(columnChunk);

	/*add a new page*/
	addDataPage(columnChunk);

	return bytes_added;
}

/**
 * add a value to a column. includes: adding r to repetition level; adding d to definition level;
 * adding the value itself to page data section
 * @chunk:			the column writer which needs to write data
 * @value:			the value needed to be inserted
 * @r:				the repetition level for the value
 * @d:				the definition level for the value
 *
 * return uncompressed bytes added to current row group
 */
int
appendParquetColumnValue(ParquetColumnChunk chunk,
						 Datum value,
						 int r,
						 int d)
{
	int bytes_added = 0;
	int encoded_len = 0;

	/*if page is null, initialize a new page*/
	if ((chunk->pageNumber == 0) || (chunk->currentPage == NULL))
	{
		addDataPage(chunk);
	}

	encoded_len = encodePlain(value,
							  chunk->currentPage,
							  chunk->columnChunkMetadata->hawqTypeId,
							  chunk->maxPageSize,
							  chunk->maxPageLimitSize);

	if (encoded_len == ENCODE_INVALID_VALUE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("value for column \"%s\" exceeds pagesize %d!",
						chunk->columnChunkMetadata->colName, chunk->maxPageLimitSize)));
	}

	if (encoded_len == ENCODE_OUTOF_PAGE)
	{
		bytes_added += finalizeCurrentAndNewPage(chunk);
		encoded_len = encodePlain(value,
								  chunk->currentPage,
								  chunk->columnChunkMetadata->hawqTypeId,
								  chunk->maxPageSize,
								  chunk->maxPageLimitSize);

	}

	bytes_added += encoded_len;

	if (chunk->currentPage->repetition_level != NULL)
	{
		RLEEncoder_WriteInt(chunk->currentPage->repetition_level, r);
	}
	if (chunk->currentPage->definition_level != NULL)
	{
		RLEEncoder_WriteInt(chunk->currentPage->definition_level, d);
	}

	chunk->currentPage->header->num_values++;
	chunk->currentPage->header->uncompressed_page_size += encoded_len;

	chunk->columnChunkMetadata->valueCount++;

	return bytes_added;
}

/*
 * Append null for field. 
 *
 * We don't consider UDT currently, therefore we don't have intermediate
 * null value. If one table's attribute is null, all its corresponding
 * columns are null, having (r,d) == (0,0).
 *
 * Return uncompressed bytes added to current row group.
 */
int
appendNullForFields(struct FileField_4C *field,
					ParquetColumnChunk columnChunks,
					int *colIndex)
{
	int bytes_added = 0;

	if (field->num_children == 0)
	{
		bytes_added += appendParquetColumnNull(&columnChunks[*colIndex]);
		*colIndex = *colIndex + 1;
	}
	else
	{
		for (int i = 0; i < field->num_children; i++)
		{
			bytes_added += appendNullForFields(&field->children[i], columnChunks, colIndex);
		}
	}

	return bytes_added;
}

/**
 * Append the value of a hawq field to parquet columns chunks. Should consider embedded types
 * repetition level and definition level calculation.
 * @field:			the hawq field for the value
 * @columnChunk:	the parquet column chunks needed to be inserted into
 * @colIndex:		current parquet column chunk index
 * @value:			the value needed to be inserted
 */
int
appendValueForFields(struct FileField_4C *field,
					 ParquetColumnChunk columnChunks,
					 int *colIndex,
					 Datum value)
{
	int bytes_added = 0;

	/*primitive type, r = 0, d = field->definition_level*/
	if (field->num_children == 0)
	{
		bytes_added += appendParquetColumnValue(columnChunks + (*colIndex),
												value,
												0, field->d);
		*colIndex = *colIndex + 1;
	}
	else
	{
		switch (field->hawqTypeId)
		{
		/* HAWQ built-in embeded type */
		case HAWQ_TYPE_POINT:
			bytes_added += appendParquetColumn_Point(columnChunks,
													 colIndex,
													 DatumGetPointP(value),
													 0, field->d);
			break;
		case HAWQ_TYPE_LSEG:
			bytes_added += appendParquetColumn_Lseg(columnChunks,
													colIndex,
													DatumGetLsegP(value),
													0, field->d);
			break;
		case HAWQ_TYPE_PATH:
			bytes_added += appendParquetColumn_Path(columnChunks,
													colIndex,
													DatumGetPathP(value),
													0, field->d);
			break;
		case HAWQ_TYPE_BOX:
			bytes_added += appendParquetColumn_Box(columnChunks,
												   colIndex,
												   DatumGetBoxP(value),
												   0, field->d);
			break;
		case HAWQ_TYPE_POLYGON:
			bytes_added += appendParquetColumn_Polygon(columnChunks,
													   colIndex,
													   DatumGetPolygonP(value),
													   0, field->d);
			break;
		case HAWQ_TYPE_CIRCLE:
			bytes_added += appendParquetColumn_Circle(columnChunks,
													  colIndex,
													  DatumGetCircleP(value),
													  0, field->d);
			break;

		default:
			/* TODO array type */
			/* TODO UDT */
			Insist(false);
			break;

		}
	}

	return bytes_added;
}

int
appendParquetColumn_Point(ParquetColumnChunk columnChunks, int *colIndex,
						  Point *point, int r, int d)
{
	int bytes_added = 0;

	/* x and y are required, there both r and d remains unchanged. */
	bytes_added += appendParquetColumnValue(columnChunks + (*colIndex),
											Float8GetDatum(point->x), r, d);
	*colIndex = *colIndex + 1;

	bytes_added += appendParquetColumnValue(columnChunks + (*colIndex),
											Float8GetDatum(point->y), r, d);
	*colIndex = *colIndex + 1;

	return bytes_added;
}

int
appendParquetColumn_Lseg(ParquetColumnChunk columnChunks, int *colIndex,
						 LSEG *lseg, int r, int d)
{
	int bytes_added = 0;

	bytes_added += appendParquetColumn_Point(columnChunks, colIndex, lseg->p, r, d);
	bytes_added += appendParquetColumn_Point(columnChunks, colIndex, lseg->p + 1, r, d);

	return bytes_added;
}

int
appendParquetColumn_Path(ParquetColumnChunk columnChunks, int *colIndex,
						 PATH *path, int r, int d)
{
	int i;
	int bytes_added = 0;
	bool is_open = !path->closed;

	/* append is_open column */
	bytes_added += appendParquetColumnValue(columnChunks + (*colIndex),
											BoolGetDatum(is_open), r, d);
	*colIndex += 1;

	/* append points.x column */
	for (i = 0; i < path->npts; ++i)
	{
		if (i == 0)
			bytes_added += appendParquetColumnValue(columnChunks + (*colIndex),
													Float8GetDatum(path->p[i].x),
													0, d + 1);
		else
			bytes_added += appendParquetColumnValue(columnChunks + (*colIndex),
													Float8GetDatum(path->p[i].x),
													r + 1, d + 1);
	}
	*colIndex += 1;

	/* append points.y column */
	for (i = 0; i < path->npts; ++i)
	{
		if (i == 0)
			bytes_added += appendParquetColumnValue(columnChunks + (*colIndex),
													Float8GetDatum(path->p[i].y),
													0, d + 1);
		else
			bytes_added += appendParquetColumnValue(columnChunks + (*colIndex),
													Float8GetDatum(path->p[i].y),
													r + 1, d + 1);
	}
	*colIndex += 1;

	return bytes_added;
}

int
appendParquetColumn_Box(ParquetColumnChunk columnChunks, int *colIndex,
						BOX *box, int r, int d)
{
	int bytes_added = 0;

	bytes_added += appendParquetColumn_Point(columnChunks, colIndex, &box->high, r, d);
	bytes_added += appendParquetColumn_Point(columnChunks, colIndex, &box->low, r, d);

	return bytes_added;
}

int
appendParquetColumn_Polygon(ParquetColumnChunk columnChunks, int *colIndex,
							POLYGON *polygon, int r, int d)
{
	int i;
	int bytes_added = 0;

	/* append boundbox:{x1,y1,x2,y2} columns */
	bytes_added += appendParquetColumn_Box(columnChunks, colIndex, &polygon->boundbox, r, d);

	/* append points:x column */
	for (i = 0; i < polygon->npts; ++i)
	{
		if (i == 0)
			bytes_added += appendParquetColumnValue(columnChunks + (*colIndex),
													Float8GetDatum(polygon->p[i].x),
													0, d + 1);
		else
			bytes_added += appendParquetColumnValue(columnChunks + (*colIndex),
													Float8GetDatum(polygon->p[i].x),
													r + 1, d + 1);
	}
	*colIndex += 1;

	/* append points:y column */
	for (i = 0; i < polygon->npts; ++i)
	{
		if (i == 0)
			bytes_added += appendParquetColumnValue(columnChunks + (*colIndex),
													Float8GetDatum(polygon->p[i].y),
													0, d + 1);
		else
			bytes_added += appendParquetColumnValue(columnChunks + (*colIndex),
													Float8GetDatum(polygon->p[i].y),
													r + 1, d + 1);
	}
	*colIndex += 1;

	return bytes_added;
}

int
appendParquetColumn_Circle(ParquetColumnChunk columnChunks, int *colIndex,
						   CIRCLE *circle,
						   int r, int d)
{
	int bytes_added = 0;

	bytes_added += appendParquetColumn_Point(columnChunks, colIndex, &circle->center, r, d);
	bytes_added += appendParquetColumnValue(columnChunks + (*colIndex), Float8GetDatum(circle->radius), r, d);
	*colIndex += 1;

	return bytes_added;
}

void
appendRowValue(ParquetRowGroup rowgroup,
			   ParquetMetadata parquetmd,
			   Datum* values, bool* nulls)
{
	int bytes_added = 0;

	/*
	 * Append row value column by column.
	 *
	 * One table's column may corresponds to multiple parquet columns
	 * due to nested data type like point, array, UDF, etc.
	 *
	 */
	int colIndex = 0;
	for (int i = 0; i < parquetmd->fieldCount; i++)
	{
		/* for null value, we insert definition level to underlying columns */
		if (nulls[i])
		{
			bytes_added += appendNullForFields(&(parquetmd->pfield[i]),
											   rowgroup->columnChunks,
											   &colIndex);
		}
		/* otherwise the actual value is written, possibly along with r/d */
		else
		{
			bytes_added += appendValueForFields(&parquetmd->pfield[i],
												rowgroup->columnChunks,
												&colIndex,
												values[i]);
		}
	}
	Assert(colIndex == parquetmd->colCount);

	rowgroup->rowGroupMetadata->totalByteSize += bytes_added;
	rowgroup->rowGroupMetadata->rowCount++;
}

/*
 * getTypeName
 *		get name of a type
 *
 * Note: any associated array type is *not* renamed; caller must make
 * another call to handle that case.  Currently this is only used for
 * renaming types associated with tables, for which there are no arrays.
 */
char *
getTypeName(Oid typeOid) {
	Relation pg_type_desc;
	HeapTuple tuple;
	Form_pg_type form;
	cqContext *pcqCtx;
	cqContext cqc;
	char *typeName;
	pg_type_desc = heap_open(TypeRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), pg_type_desc);

	tuple = caql_getfirst(
			pcqCtx,
			cql("SELECT typname FROM pg_type "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(typeOid)));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type with OID \"%d\" does not exist", typeOid)));

	form = (Form_pg_type) GETSTRUCT(tuple);
	typeName = (char*) palloc0(strlen(form->typname.data) + 1);
	memcpy(typeName, form->typname.data, strlen(form->typname.data));

	heap_freetuple(tuple);
	heap_close(pg_type_desc, RowExclusiveLock);
	return typeName;
}

/*
 * Before finalize a page, we cannot know the exact number of
 * uncompressed size of a page, due to rle/bitpack encoder buffers
 * some input value.
 *
 * This producure returns an approximate uncompressed page size which
 * mey be a little smaller than the actual size.
 */
int
approximatePageSize(ParquetDataPage page)
{
	int size = page->header->uncompressed_page_size;

	if (page->repetition_level != NULL)
		size += RLEEncoder_Size(page->repetition_level);

	if (page->definition_level != NULL)
		size += RLEEncoder_Size(page->definition_level);

	return size;
}

/*
 * hcatalogmd.h
 *
 *  Representation of hcatalog table metadata
 *  Created on: Mar 6, 2015
 *      Author: antova
 */

#ifndef HCATALOGMD_H_
#define HCATALOGMD_H_

#include "postgres.h"
#include "nodes/parsenodes.h"

/*
 * Metadata for columns in HCatalog tables
 */
typedef struct HCatalogColumn
{
	/* column name */
	char *colName;
	
	/* type name */
	char *typeName;
	
	/* type modifiers, e.g. max length or precision */
	int typeModifiers[2];
	
	/* number of type modifiers */
	int nTypeModifiers;
} HCatalogColumn;

/*
 * Metadata for HCatalog tables
 */
typedef struct HCatalogTable
{
	/* database name */
	char *dbName;

	/* table name */
	char *tableName;
	
	/* columns */
	List *columns;
} HCatalogTable;



#endif /* HCATALOGMD_H_ */

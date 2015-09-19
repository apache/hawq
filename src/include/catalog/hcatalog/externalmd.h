/*
 * externalmd.h
 *
 *  Created on: Mar 6, 2015
 *      Author: antova
 */

#ifndef EXTERNALMD_H_
#define EXTERNALMD_H_

#include "postgres.h"
#include "catalog/hcatalog/hcatalogmd.h"
#include "nodes/parsenodes.h"

extern List *ParseHCatalogEntries(StringInfo json);

#endif /* EXTERNALMD_H_ */

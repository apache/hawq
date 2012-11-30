/*
 * parse_cte.h
 *    Handle WITH clause in parser.
 *
 * Copyright (c) 2011 - present, EMC Greenplum.
 */
#ifndef PARSE_CTE_H
#define PARSE_CTE_H

#include "parser/parse_node.h"

extern List *transformWithClause(ParseState *pstate, WithClause *withClause);
extern CommonTableExpr *GetCTEForRTE(ParseState *pstate, RangeTblEntry *rte, int rtelevelsup);

#endif

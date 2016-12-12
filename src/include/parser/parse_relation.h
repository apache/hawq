/*-------------------------------------------------------------------------
 *
 * parse_relation.h
 *	  prototypes for parse_relation.c.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/parser/parse_relation.h,v 1.55 2006/10/04 00:30:09 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_RELATION_H
#define PARSE_RELATION_H

#include "parser/parse_node.h"

#define ERRMSG_GP_WITH_COLUMNS_MISMATCH \
	"specified number of columns in WITH query \"%s\" must not " \
	"exceed the number of available columns"

extern bool add_missing_from;

extern RangeTblEntry *refnameRangeTblEntry(ParseState *pstate,
					 const char *catalogname, 
					 const char *schemaname,
					 const char *refname,
					 int location,
					 int *sublevels_up);
extern void checkNameSpaceConflicts(ParseState *pstate, List *namespace1,
						List *namespace2);
extern CommonTableExpr *scanNameSpaceForCTE(ParseState *pstate,
											const char *refname,
											Index *ctelevelsup);
extern int RTERangeTablePosn(ParseState *pstate,
				  RangeTblEntry *rte,
				  int *sublevels_up);
extern RangeTblEntry *GetRTEByRangeTablePosn(ParseState *pstate,
					   int varno,
					   int sublevels_up);
extern Node *scanRTEForColumn(ParseState *pstate, RangeTblEntry *rte,
				 char *colname, int location);
extern Node *colNameToVar(ParseState *pstate, char *colname, bool localonly,
			 int location);
extern Node *qualifiedNameToVar(ParseState *pstate,
                                   char *catalogname, 
                                   char *schemaname,
				   char *refname,
				   char *colname,
				   bool implicitRTEOK,
				   int location);
extern RangeTblEntry *addRangeTableEntry(ParseState *pstate,
				   RangeVar *relation,
				   Alias *alias,
				   bool inh,
				   bool inFromCl);
extern RangeTblEntry *addRangeTableEntryForRelation(ParseState *pstate,
							  Relation rel,
							  Alias *alias,
							  bool inh,
							  bool inFromCl);
extern RangeTblEntry *addRangeTableEntryForSubquery(ParseState *pstate,
							  Query *subquery,
							  Alias *alias,
							  bool inFromCl);
extern RangeTblEntry *addRangeTableEntryForFunction(ParseState *pstate,
							  char *funcname,
							  Node *funcexpr,
							  RangeFunction *rangefunc,
							  bool inFromCl);
extern RangeTblEntry *addRangeTableEntryForValues(ParseState *pstate,
							List *exprs,
							Alias *alias,
							bool inFromCl);
extern RangeTblEntry *addRangeTableEntryForJoin(ParseState *pstate,
						  List *colnames,
						  JoinType jointype,
						  List *aliasvars,
						  Alias *alias,
						  bool inFromCl);
extern RangeTblEntry *addRangeTableEntryForCTE(ParseState *pstate,
											   CommonTableExpr *cte,
											   Index levelsup,
											   RangeVar *rangeVar,
											   bool inFromCl);
extern bool isSimplyUpdatableRelation(Oid relid);
extern Index extractSimplyUpdatableRTEIndex(List *rtable);
extern void addRTEtoQuery(ParseState *pstate, RangeTblEntry *rte,
			  bool addToJoinList,
			  bool addToRelNameSpace, bool addToVarNameSpace);
extern RangeTblEntry *addImplicitRTE(ParseState *pstate, RangeVar *relation,
			   int location);
extern void expandRTE(RangeTblEntry *rte, int rtindex, int sublevels_up,
		  int location, bool include_dropped,
		  List **colnames, List **colvars);
extern List *expandRelAttrs(ParseState *pstate, RangeTblEntry *rte,
			   int rtindex, int sublevels_up, int location);
extern int	attnameAttNum(Relation rd, const char *attname, bool sysColOK);
extern Name attnumAttName(Relation rd, int attid);
extern Oid	attnumTypeId(Relation rd, int attid);

extern void ExecCheckRTPerms(List *rangeTable);
extern void ExecCheckRTPermsWithRanger(List *);
extern void ExecCheckRTEPerms(RangeTblEntry *rte);

#endif   /* PARSE_RELATION_H */

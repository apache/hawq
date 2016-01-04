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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * readfuncs.h
 *	  header file for read.c and readfuncs.c. These functions are internal
 *	  to the stringToNode interface and should not be used by anyone else.
 *
 * Portions Copyright (c) 2006-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/nodes/readfuncs.h,v 1.22 2006/03/05 15:58:57 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef READFUNCS_H
#define READFUNCS_H

#include "nodes/nodes.h"

/*
 * prototypes for functions in read.c (the lisp token parser)
 */
extern char *pg_strtok(int *length);
extern char *debackslash(char *token, int length);
extern void *nodeRead(char *token, int tok_len);

/*
 * nodeReadSkip
 *    Skips next item (a token, list or subtree).
 */
void
nodeReadSkip(void);

/*
 * pg_strtok_peek_fldname
 *    Peeks at the token that will be returned by the next call to
 *    pg_strtok.  Returns true if the token is, case-sensitively,
 *          :fldname
 */
bool
pg_strtok_peek_fldname(const char *fldname);

/*
 * pg_strtok_prereq
 *    If the next tokens to be returned by pg_strtok are, case-sensitively,
 *          :prereq <featurename>
 *    then this function consumes them and returns true.  Otherwise false
 *    is returned and no tokens are consumed.
 */
bool
pg_strtok_prereq(const char *featurename);


/*-------------------------------------------------------------------------
 * prototypes for functions in readfuncs.c
 */
extern Node *parseNodeString(void);

#endif   /* READFUNCS_H */

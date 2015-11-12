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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*-------------------------------------------------------------------------
 *
 * cdbdef.h
 *	Definitions for use anywhere
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBDEF_H
#define CDBDEF_H


/*
 * CdbSwap
 *		Exchange the contents of two variables.
 */
#define CdbSwap(_type, _x, _y)	\
	do							\
	{							\
		_type _t = (_x);		\
		_x = (_y);				\
		_y = (_t);				\
	} while (0)


/*
 * CdbVisitOpt
 *      Some tree walkers use these codes to direct the traversal.
 */
typedef enum 
{
    CdbVisit_Walk = 1,          /* proceed in normal sequence */    
    CdbVisit_Skip,              /* no more calls for current node or its kids */
    CdbVisit_Stop,              /* break out of traversal, no more callbacks */
    CdbVisit_Failure,           /* break out of traversal, no more callbacks */
    CdbVisit_Success            /* break out of traversal, no more callbacks */
} CdbVisitOpt;


#endif   /* CDBDEF_H */

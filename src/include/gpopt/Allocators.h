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

//---------------------------------------------------------------------------
//	@filename:
//		Allocators.h
//
//	@doc:
//		Memory allocation/deallocation operators
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_Allocators_H
#define GPOS_Allocators_H

#include "gpos/memory/IMemoryPool.h"

using namespace gpos;

//---------------------------------------------------------------------------
// Overloading global new operators
//---------------------------------------------------------------------------


// throwing global singleton new operator
void* operator new
	(
	gpos::SIZE_T cSize
	)
	throw(gpos::BAD_ALLOC);


// throwing global array new operator
void* operator new []
	(
	gpos::SIZE_T cSize
	)
	throw(gpos::BAD_ALLOC);


// non-throwing global singleton new operator
void *operator new
	(
	gpos::SIZE_T  cSize,
	const gpos::NO_THROW &
	)
	throw();


// non-throwing global array new operator
void* operator new []
	(
	gpos::SIZE_T cSize,
	const gpos::NO_THROW &
	)
	throw();

//---------------------------------------------------------------------------
// Overloading new/delete to implement placement variants
//---------------------------------------------------------------------------


// placement new operator
void *operator new
	(
	gpos::SIZE_T cSize,
	gpos::IMemoryPool *pmp,
	const gpos::CHAR *szFilename,
	gpos::ULONG cLine
	);

// placement array new operator
void *operator new []
	(
	gpos::SIZE_T cSize,
	gpos::IMemoryPool *pmp,
	const gpos::CHAR *szFilename,
	gpos::ULONG cLine
	);



//---------------------------------------------------------------------------
// 	Must provide two variants for delete:
//		1. one to be used in constructors
//		2. one in all other situations
//
//	Internally both map to the same delete function;
//---------------------------------------------------------------------------

// placement delete
void operator delete (void *pv) throw();
void operator delete
	(
	void *pv,
	gpos::IMemoryPool *pmp,
	const gpos::CHAR *szFilename,
	gpos::ULONG cLine
	);

// placement array delete
void operator delete [] (void *pv) throw();
void operator delete []
	(
	void *pv,
	gpos::IMemoryPool *pmp,
	const gpos::CHAR *szFilename,
	gpos::ULONG cLine
	);


// non-throwing singleton delete operator
void operator delete
	(
	void* pv,
	const gpos::NO_THROW&
	)
	throw();

// non-throwing array delete operator
void operator delete[]
	(
	void* pv,
	const gpos::NO_THROW&
	)
	throw();

#endif // !GPOS_Allocators_H

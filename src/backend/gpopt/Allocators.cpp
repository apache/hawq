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

#include "gpopt/Allocators.h"
#include "gpos/memory/CMemoryPoolManager.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		new
//
//	@doc:
//		Overloaded throwing global singleton new operator
//
//---------------------------------------------------------------------------
void* operator new
	(
	SIZE_T cSize
	)
	throw(BAD_ALLOC)
{
	return NewImpl
				(
				CMemoryPoolManager::Pmpm()->PmpGlobal(),
				cSize,
				NULL, // szFileName
				0, // ulLine
				IMemoryPool::EatSingleton
				);
}


//---------------------------------------------------------------------------
//	@function:
//		new[]
//
//	@doc:
//		Overloaded throwing global array new operator
//
//---------------------------------------------------------------------------
void* operator new []
	(
	SIZE_T cSize
	)
	throw(BAD_ALLOC)
{
	return NewImpl
				(
				CMemoryPoolManager::Pmpm()->PmpGlobal(),
				cSize,
				NULL, // szFileName
				0, // ulLine
				IMemoryPool::EatArray
				);
}


//---------------------------------------------------------------------------
//	@function:
//		new
//
//	@doc:
//		Overloaded non-throwing global singleton new operator
//
//---------------------------------------------------------------------------
void *operator new
	(
	SIZE_T  cSize,
	const NO_THROW &
	)
	throw()
{
	return NewImplNoThrow
				(
				CMemoryPoolManager::Pmpm()->PmpGlobal(),
				cSize,
				NULL, // szFileName
				0, // ulLine
				IMemoryPool::EatSingleton
				);
}


//---------------------------------------------------------------------------
//	@function:
//		new
//
//	@doc:
//		Overloaded non-throwing global array new operator
//
//---------------------------------------------------------------------------
void* operator new []
	(
	SIZE_T cSize,
	const NO_THROW &
	)
	throw()
{
	return NewImplNoThrow
				(
				CMemoryPoolManager::Pmpm()->PmpGlobal(),
				cSize,
				NULL, // szFileName
				0, // ulLine
				IMemoryPool::EatArray
				);
}


//---------------------------------------------------------------------------
//	@function:
//		new
//
//	@doc:
//		overloaded placement new operator
//
//---------------------------------------------------------------------------
void *
operator new
	(
	SIZE_T cSize,
	IMemoryPool *pmp,
	const CHAR *szFilename,
	ULONG ulLine
	)
{
	return NewImpl(pmp, cSize, szFilename, ulLine, IMemoryPool::EatSingleton);
}


//---------------------------------------------------------------------------
//	@function:
//		new[]
//
//	@doc:
//		Overload for array allocation; raises OOM exception if
//		unable to allocate
//
//---------------------------------------------------------------------------
void *
operator new []
	(
	SIZE_T cSize,
	IMemoryPool *pmp,
	const CHAR *szFilename,
	ULONG ulLine
	)
{
	return NewImpl(pmp, cSize, szFilename, ulLine, IMemoryPool::EatArray);
}


//---------------------------------------------------------------------------
//	@function:
//		delete
//
//	@doc:
//		Overload for singleton deletion
//
//---------------------------------------------------------------------------
void
operator delete
	(
	void *pv
	)
	throw()
{
	DeleteImpl(pv, IMemoryPool::EatSingleton);
}


//---------------------------------------------------------------------------
//	@function:
//		delete
//
//	@doc:
//		Placement delete; only used if constructor throws
//
//---------------------------------------------------------------------------
void
operator delete
	(
	void *pv,
	IMemoryPool *, // pmp,
	const CHAR *, // szFilename,
	ULONG // ulLine
	)
{
	DeleteImpl(pv, IMemoryPool::EatSingleton);
}


//---------------------------------------------------------------------------
//	@function:
//		delete []
//
//	@doc:
//		Overload for array deletion
//
//---------------------------------------------------------------------------
void
operator delete []
	(
	void *pv
	)
	throw()
{
	DeleteImpl(pv, IMemoryPool::EatArray);
}


//---------------------------------------------------------------------------
//	@function:
//		delete []
//
//	@doc:
//		Placement delete []; only used if constructor throws
//
//---------------------------------------------------------------------------
void
operator delete []
	(
	void *pv,
	IMemoryPool *, // pmp,
	const CHAR *, // szFilename,
	ULONG // ulLine
	)
{
	DeleteImpl(pv, IMemoryPool::EatArray);
}


//---------------------------------------------------------------------------
//	@function:
//		delete
//
//	@doc:
//		Non-throwing singleton delete operator
//
//---------------------------------------------------------------------------
void
operator delete
	(
	void* pv,
	const gpos::NO_THROW&
	)
	throw()
{
	DeleteImplNoThrow(pv, IMemoryPool::EatSingleton);
}


//---------------------------------------------------------------------------
//	@function:
//		delete
//
//	@doc:
//		Non-throwing array delete operator
//
//---------------------------------------------------------------------------
void
operator delete []
	(
	void* pv,
	const gpos::NO_THROW&
	)
	throw()
{
	DeleteImplNoThrow(pv, IMemoryPool::EatArray);
}


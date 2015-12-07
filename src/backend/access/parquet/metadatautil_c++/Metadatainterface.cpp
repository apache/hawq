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


#include <iostream>
#include <stdint.h>
#include <boost/shared_ptr.hpp>
#include <boost/math_fwd.hpp>
#include <thrift/protocol/TBinaryProtocol.h>
#include <fcntl.h>
#include <fstream>
#include <stdio.h>
#include <bitset>

extern "C" {
#include "postgres.h"
}

#include "MetadataUtil.h"
using namespace hawq;
using namespace std;
using namespace boost;

extern "C" {
#include "utils/palloc.h"

/*
 * Read buffer to get page metadata
 */
int
readPageMetadata(
		uint8_t 				*buf,
		uint32_t				*len,
		int 					compact,
		struct PageMetadata_4C	**ppageMetdata)
{
	*ppageMetdata = (struct PageMetadata_4C*)palloc0(sizeof(struct PageMetadata_4C));
	bool compactBool = (compact == 1) ? true : false;
	int iret = MetadataUtil::readPageMetadata(buf, len, compactBool, *ppageMetdata);
	return iret;
}

/*
 * Write hawq page metadata to buffer
 */
int
writePageMetadata(
		uint8_t					**buf,
		uint32_t				*len,
		struct PageMetadata_4C	*ppageMetadata)
{
	int iret = MetadataUtil::writePageMetadata(buf, len, ppageMetadata);
	return iret;
}

/*
 * Write hawq column chunk metadata to buffer
 */
int
writeColumnChunkMetadata(
		uint8_t							**buf,
		uint32_t						*len,
		struct ColumnChunkMetadata_4C	*blockMetadata)
{
	int iret = MetadataUtil::writeColumnChunkMetadata(buf, len, blockMetadata);
	return iret;
}
}

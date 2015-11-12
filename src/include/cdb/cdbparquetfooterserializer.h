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

/*
 * cdbparquetfooterserializer.h
 *
 *  Created on: Mar 17, 2014
 *      Author: malili
 */

#ifndef CDBPARQUETFOOTERSERIALIZER_H_
#define CDBPARQUETFOOTERSERIALIZER_H_
#include "postgres.h"
#include "storage/fd.h"
#include "cdb/cdbparquetfooterprocessor.h"

/**
 * Deserialize part functions: read from parquet file
 */

/* Initialize deserialize footer from file, read one rowgroup information once */
void
initDeserializeFooter(
		File file,
		int64 footerLength,
		char *fileName,
		ParquetMetadata *parquetMetadata,
		CompactProtocol **footerProtocol);

/* Get next row group metadata information */
void
readNextRowGroupInfo(
		ParquetMetadata parquetMetadata,
		CompactProtocol *prot);

/* End of deserialize*/
void
endDeserializerFooter(
		ParquetMetadata parquetMetadata,
		CompactProtocol **prot);

void freeFooterProtocol(CompactProtocol *protocol);


/**
 * Deserialize part functions: write to parquet file
 */
void
initSerializeFooter(
		CompactProtocol **footerProtocol,
		char *fileName);

int
endSerializeFooter(CompactProtocol **read_prot,
		CompactProtocol **write_prot,
		char	*fileName,
		File	file,
		ParquetMetadata parquetMetadata,
		int		rowgroup_cnt);

int
writeRowGroupInfo(
		struct BlockMetadata_4C* rowGroupInfo,
		CompactProtocol *prot);

/**
 * Free metadata part functions
 */

void
freeRowGroupInfo(
		struct BlockMetadata_4C *blockMetadata);

void
freeParquetMetadata(
		ParquetMetadata parquetMetadata);



#endif /* CDBPARQUETFOOTERSERIALIZER_H_ */

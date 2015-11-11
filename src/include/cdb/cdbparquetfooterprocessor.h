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
 * cdbparquetfooterprocessor.h
 *
 *  Created on: Sep 22, 2013
 *      Author: malili
 */

#ifndef CDBPARQUETFOOTERPROCESSOR_H_
#define CDBPARQUETFOOTERPROCESSOR_H_

#include "postgres.h"
#include "storage/fd.h"
#include "access/parquetmetadata_c++/MetadataInterface.h"
#include "access/tupdesc.h"
#include "cdb/cdbparquetfooterserializer_protocol.h"

#define CURRENT_PARQUET_VERSION 1

typedef struct ParquetMetadata_4C *ParquetMetadata;

void DetectHostEndian(void);

void writeParquetHeader(File dataFile, char *filePathName, int64 *fileLen,
		int64 *fileLen_uncompressed);

void writeParquetFooter(File dataFile,
		char *filePathName,
		ParquetMetadata parquetMetadata,
		int64 *fileLen,
		int64 *fileLen_uncompressed,
		CompactProtocol **footer_read_protocol,
		CompactProtocol **footer_write_protocol,
		int	previous_rowgroup_count);

bool readParquetFooter(File fileHandler, ParquetMetadata *parquetMetadata,
		CompactProtocol **footerProtocol, int64 eof, char *filePathName);

bool checkAndSyncMetadata(ParquetMetadata parquetmd, TupleDesc tupdesc);

#endif /* CDBPARQUETFOOTERPROCESSOR_H_ */

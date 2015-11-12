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

#ifndef RESOURCE_MANAGER_FILE_UTILITIES_H
#define RESOURCE_MANAGER_FILE_UTILITIES_H
#include "envswitch.h"

/**
 *         |<----------- 64 bits (8 bytes) ----------->|
 * 		   +---------------------+----+-----+----+-----+
 * 		   |    Instance Size	 | D  | E   | R  |  W  |
 * 		   +---------------------+----+-----+----+-----+
 * 		   |       Name Len      | Acc Mask | Reserved |
 * 		   +---------------------+----------+----------+
 *         |           Write Bytes per second          |
 *         +-------------------------------------------+
 *         |           Read  Bytes per second          |
 *         +-------------------------------------------+
 *		   |           Name string                     |
 *		   |                                           |
 *		   +-------------------------------------------+
 */

enum UINT8_STAT_VALUES {
	STAT_FALSE = 0,
	STAT_TRUE  = 1,
	STAT_UNSET = 0xFF
};

struct FileConfigurationData {
	uint32_t	InstanceSize;
	uint8_t		isDirectory;
	uint8_t 	Exist;
	uint8_t 	Readable;
	uint8_t 	Writable;
	uint32_t 	FileNameLength;
	uint16_t	AccessMask;
	uint16_t	Reserved;
	double		WriteBytesPerSec;
	double		ReadBytesPerSec;
	char		FileName[1];
};

typedef struct FileConfigurationData  *FileConfiguration;
typedef struct FileConfigurationData   FileConfigurationData;

#define UTIL_FILESYSTEM_FAIL_GET_FILESTAT 1

FileConfiguration createFileConfiguration(MCTYPE context, char *filename);

int updateFileConfigurationAsLocalFile(FileConfiguration fileconfig, bool testio);

#endif /* RESOURCE_MANAGER_FILE_UTILITIES_H */

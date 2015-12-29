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
#ifndef GFILE_H
#define GFILE_H

/*#include "c.h"*/
#include <sys/types.h>
#ifndef WIN32
#include <bzlib.h>
#include <zlib.h>
#endif

#ifdef WIN32
#include <windows.h>
#endif
#ifdef WIN32
#ifndef _WIN64
typedef long ssize_t;
#else
typedef _int64 ssize_t;
#endif
#endif

#ifdef WIN32
typedef BOOL bool_t;
#else
typedef char bool_t;
#ifndef FALSE
#define FALSE 0
#endif
#ifndef TRUE
#define TRUE 1
#endif
#endif

struct gpfxdist_t;

typedef enum Compression_type
{
	NO_COMPRESSION = 0,
	GZ_COMPRESSION,
	BZ_COMPRESSION
} compression_type;

/* The struct gfile_t is private.  Please do not use any of its fields. */
typedef struct gfile_t
{
	ssize_t(*read)(struct gfile_t*,void*,size_t);
	ssize_t(*write)(struct gfile_t*,void*,size_t);
	int(*close)(struct gfile_t*);
	off_t compressed_size,compressed_position;
	bool_t is_win_pipe;

	union
	{
		int filefd;
#ifdef WIN32
		HANDLE pipefd;
#endif
	} fd;

	union
	{
		int txt;
#ifndef WIN32
		struct zlib_stuff*z;
		struct bzlib_stuff*bz;
#endif
	}u;
	bool_t is_write;
	compression_type compression;

	struct gpfxdist_t* transform;
}gfile_t;

/*
 * MPP-13817 (support opening files without O_SYNC)
 */
int gfile_open_flags(int writing, int usesync);
#define GFILE_OPEN_FOR_READ  	    0
#define GFILE_OPEN_FOR_WRITE_NOSYNC 1
#define GFILE_OPEN_FOR_WRITE_SYNC   2

int gfile_open(gfile_t* fd, const char* fpath, int flags, int* response_code, const char** response_string, struct gpfxdist_t* transform);
int gfile_close(gfile_t*fd);
off_t gfile_get_compressed_size(gfile_t*fd);
off_t gfile_get_compressed_position(gfile_t*fd);
ssize_t gfile_read(gfile_t* fd, void* ptr, size_t len); /* gfile_read reads as much as it can--short read indicates error. */
ssize_t gfile_write(gfile_t* fd, void* ptr, size_t len);
void gfile_printf_then_putc_newline(const char*format,...) __attribute__ ((__format__ (__printf__, 1, 0)));
void*gfile_malloc(size_t size);
void gfile_free(void*a);

#endif

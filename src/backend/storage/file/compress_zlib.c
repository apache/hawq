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
/* compress_zlib.c */
#include "c.h"
#include "storage/bfz.h"
#include <zlib.h>

struct bfz_zlib_freeable_stuff
{
	struct bfz_freeable_stuff super;
	gzFile		f;
};

/* This file implements bfz compression algorithm "zlib". */

/*
 * bfz_zlib_close_ex
 *  Close a file and freeing up descriptor, buffers etc.
 *
 *  This is also called from an xact end callback, hence it should
 *  not contain any elog(ERROR) calls.
 */
static void
bfz_zlib_close_ex(bfz_t * thiz)
{
	struct bfz_zlib_freeable_stuff *fs = (void *) thiz->freeable_stuff;

	if (fs->f)
	{
		/* gzclose also closes thiz->fd */
		gzclose(fs->f);
		thiz->fd = -1;
	}
	free(fs);
	thiz->freeable_stuff = NULL;
}

static void
gzwrite_fully(gzFile f, const char *buffer, int size)
{
	while (size)
	{
		int			i = gzwrite(f, (void *) buffer, size);

		if (i <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					errmsg("could not write to temporary file: %m")));
		buffer += i;
		size -= i;
	}
}

static int
gzread_fully(gzFile f, char *buffer, int size)
{
	int			orig_size = size;

	while (size)
	{
		int			i = gzread(f, buffer, size);

		if (i < 0)
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					errmsg("could not read from temporary file: %m")));
		if (i == 0)
			break;
		buffer += i;
		size -= i;
	}
	return orig_size - size;
}

static void
bfz_zlib_write_ex(bfz_t * thiz, const char *buffer, int size)
{
	struct bfz_zlib_freeable_stuff *fs = (void *) thiz->freeable_stuff;

	gzwrite_fully(fs->f, buffer, size);
}

static int
bfz_zlib_read_ex(bfz_t * thiz, char *buffer, int size)
{
	struct bfz_zlib_freeable_stuff *fs = (void *) thiz->freeable_stuff;

	return gzread_fully(fs->f, buffer, size);
}

void
bfz_zlib_init(bfz_t * thiz)
{
	struct bfz_zlib_freeable_stuff *fs = malloc(sizeof *fs);

	if (!fs)
		ereport(ERROR,
			(errcode(ERRCODE_OUT_OF_MEMORY),
			 errmsg("out of memory")));

	thiz->freeable_stuff = &fs->super;
	fs->super.read_ex = bfz_zlib_read_ex;
	fs->super.write_ex = bfz_zlib_write_ex;
	fs->super.close_ex = bfz_zlib_close_ex;

	if (thiz->mode == BFZ_MODE_APPEND)
		fs->f = gzdopen(thiz->fd, "wb1");
	else
		fs->f = gzdopen(thiz->fd, "rb");

	if (!fs->f)
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				errmsg("could not open temporary file: %m")));
}

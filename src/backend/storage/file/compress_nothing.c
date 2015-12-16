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
/* compress_nothing.c */

#include "postgres.h"

#include "c.h"
#include <unistd.h>
#include <storage/bfz.h>
#include <storage/fd.h>

/*
 * This file implements bfz compression algorithm "nothing".
 * We don't compress at all.
 */

/*
 * bfz_nothing_close_ex
 *  Close a file and freeing up descriptor, buffers etc.
 *
 *  This is also called from an xact end callback, hence it should
 *  not contain any elog(ERROR) calls.
 */
static void
bfz_nothing_close_ex(bfz_t * thiz)
{
	gp_retry_close(thiz->fd);
	thiz->fd = -1;
	free(thiz->freeable_stuff);
	thiz->freeable_stuff = NULL;
}

static int
bfz_nothing_read_ex(bfz_t * thiz, char *buffer, int size)
{
	int			orig_size = size;

	while (size)
	{
		int			i = readAndRetry(thiz->fd, buffer, size);

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
bfz_nothing_write_ex(bfz_t * bfz, const char *buffer, int size)
{
	while (size)
	{
		int			i = writeAndRetry(bfz->fd, buffer, size);

		if (i < 0)
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					errmsg("could not write to temporary file: %m")));
		buffer += i;
		size -= i;
	}
}

void
bfz_nothing_init(bfz_t * thiz)
{
	struct bfz_freeable_stuff *fs = malloc(sizeof *fs);

	if (!fs)
		ereport(ERROR,
			(errcode(ERRCODE_OUT_OF_MEMORY),
			 errmsg("out of memory")));

	thiz->freeable_stuff = fs;

	fs->read_ex = bfz_nothing_read_ex;
	fs->write_ex = bfz_nothing_write_ex;
	fs->close_ex = bfz_nothing_close_ex;
}

/* compress_threaded.c */
#include "c.h"
#include "storage/bfz.h"

static void
bfz_threaded_close_ex(bfz_t * thiz)
{
	struct bfz_threaded_freeable_stuff *fs = (void *) thiz->freeable_stuff;

	if (thiz->mode == BFZ_MODE_APPEND)
		pipe_done_writing(&fs->pipe);
	else if (thiz->mode == BFZ_MODE_SCAN)
		while (pipe_read(&fs->pipe, fs->super.buffer, sizeof fs->super.buffer) != 0);

	pthread_join(fs->thread, NULL);
	pipe_close(&fs->pipe);
	free(fs);
	thiz->freeable_stuff = NULL;
}

static void
bfz_threaded_write_ex(bfz_t * thiz, const char *buffer, int size)
{
	struct bfz_threaded_freeable_stuff *fs = (void *) thiz->freeable_stuff;

	pipe_write(&fs->pipe, buffer, size);
}

static int
bfz_threaded_read_ex(bfz_t * thiz, char *buffer, int size)
{
	struct bfz_threaded_freeable_stuff *fs = (void *) thiz->freeable_stuff;

	return pipe_read(&fs->pipe, buffer, size);
}

void
bfz_threaded_init(bfz_t * thiz, void *(*compress) (void *), void *(*uncompress) (void *))
{
	struct bfz_threaded_freeable_stuff *fs = malloc(sizeof *fs);

	if (!fs)
		ereport(ERROR,
			(errcode(ERRCODE_OUT_OF_MEMORY),
			 errmsg("out of memory")));


	thiz->freeable_stuff = &fs->super;

	fs->super.read_ex = bfz_threaded_read_ex;
	fs->super.write_ex = bfz_threaded_write_ex;
	fs->super.close_ex = bfz_threaded_close_ex;

	pipe_init(&fs->pipe);

	if (pthread_create(&fs->thread, NULL, thiz->mode == BFZ_MODE_APPEND ? compress : uncompress, thiz))
		insist_log(false, "bfz: pthread_create failed");
}

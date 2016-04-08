/*
 * $PostgreSQL: pgsql/src/tools/fsync/test_fsync.c,v 1.23.2.2 2009/09/21 20:21:02 momjian Exp $
 *
 *
 *	test_fsync.c
 *		test various fsync() methods
 */

#include "postgres.h"

#include "access/xlog_internal.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>

/*
 * Limitation of buffer-alignment for direct IO depends on OS and filesystem,
 * but XLOG_BLCKSZ is assumed to be enough for it.
 */
#ifdef O_DIRECT
#define ALIGNOF_XLOG_BUFFER     XLOG_BLCKSZ
#else
#define ALIGNOF_XLOG_BUFFER     ALIGNOF_BUFFER
#endif 

#ifdef WIN32
#define FSYNC_FILENAME	"./test_fsync.out"
#else
/* /tmp might be a memory file system */
#define FSYNC_FILENAME	"/var/tmp/test_fsync.out"
#endif

#define WRITE_SIZE	(16 * 1024) /* 16k */

void		die(char *str);
void		print_elapse(struct timeval start_t, struct timeval elapse_t);

int
main(int argc, char *argv[])
{
	struct timeval start_t;
	struct timeval elapse_t;
	int			tmpfile,
				i,
				loops = 1000;
	char	   *full_buf = (char *) malloc(XLOG_SEG_SIZE),
			   *buf;
	char	   *filename = FSYNC_FILENAME;

	if (argc > 2 && strcmp(argv[1], "-f") == 0)
	{
		filename = argv[2];
		argv += 2;
		argc -= 2;
	}

	if (argc > 1)
		loops = atoi(argv[1]);

	for (i = 0; i < XLOG_SEG_SIZE; i++)
		full_buf[i] = 'a';

	if ((tmpfile = open(filename, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR)) == -1)
		die("Cannot open output file.");
	if (write(tmpfile, full_buf, XLOG_SEG_SIZE) != XLOG_SEG_SIZE)
		die("write failed");
	/* fsync so later fsync's don't have to do it */
	if (fsync(tmpfile) != 0)
		die("fsync failed");
	close(tmpfile);

	buf = (char *) TYPEALIGN(ALIGNOF_XLOG_BUFFER, full_buf);

	/*
	 *	Simple write
	 */
	printf("Simple write timing:\n");
	/* write only */
	gettimeofday(&start_t, NULL);
	for (i = 0; i < loops; i++)
	{
		if ((tmpfile = open(filename, O_RDWR, 0)) == -1)
			die("Cannot open output file.");
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		close(tmpfile);
	}
	gettimeofday(&elapse_t, NULL);
	printf("\twrite                  ");
	print_elapse(start_t, elapse_t);
	printf("\n");

	/*
	 *	Fsync another file descriptor?
	 */
	printf("\nCompare fsync times on write() and non-write() descriptor:\n");
	printf("If the times are similar, fsync() can sync data written\non a different descriptor.\n");

	/* write, fsync, close */
	gettimeofday(&start_t, NULL);
	for (i = 0; i < loops; i++)
	{
		if ((tmpfile = open(filename, O_RDWR, 0)) == -1)
			die("Cannot open output file.");
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		if (fsync(tmpfile) != 0)
			die("fsync failed");
		close(tmpfile);
		if ((tmpfile = open(filename, O_RDWR, 0)) == -1)
			die("Cannot open output file.");
		/* do nothing but the open/close the tests are consistent. */
		close(tmpfile);
	}
	gettimeofday(&elapse_t, NULL);
	printf("\twrite, fsync, close    ");
	print_elapse(start_t, elapse_t);
	printf("\n");

	/* write, close, fsync */
	gettimeofday(&start_t, NULL);
	for (i = 0; i < loops; i++)
	{
		if ((tmpfile = open(filename, O_RDWR, 0)) == -1)
			die("Cannot open output file.");
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		close(tmpfile);
		/* reopen file */
		if ((tmpfile = open(filename, O_RDWR, 0)) == -1)
			die("Cannot open output file.");
		if (fsync(tmpfile) != 0)
			die("fsync failed");
		close(tmpfile);
	}
	gettimeofday(&elapse_t, NULL);
	printf("\twrite, close, fsync    ");
	print_elapse(start_t, elapse_t);
	printf("\n");

	/*
	 *	Compare 1 to 2 writes
	 */
	printf("\nCompare one o_sync write to two:\n");

#ifdef OPEN_SYNC_FLAG
	/* 16k o_sync write */
	if ((tmpfile = open(filename, O_RDWR | OPEN_SYNC_FLAG, 0)) == -1)
		die("Cannot open output file.");
	gettimeofday(&start_t, NULL);
	for (i = 0; i < loops; i++)
	{
		if (write(tmpfile, buf, WRITE_SIZE) != WRITE_SIZE)
			die("write failed");
		if (lseek(tmpfile, 0, SEEK_SET) == -1)
			die("seek failed");
	}
	gettimeofday(&elapse_t, NULL);
	close(tmpfile);
	printf("\tone 16k o_sync write   ");
	print_elapse(start_t, elapse_t);
	printf("\n");

	/* Two 8k o_sync writes */
	if ((tmpfile = open(filename, O_RDWR | OPEN_SYNC_FLAG, 0)) == -1)
		die("Cannot open output file.");
	gettimeofday(&start_t, NULL);
	for (i = 0; i < loops; i++)
	{
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		if (lseek(tmpfile, 0, SEEK_SET) == -1)
			die("seek failed");
	}
	gettimeofday(&elapse_t, NULL);
	close(tmpfile);
	printf("\ttwo 8k o_sync writes   ");
	print_elapse(start_t, elapse_t);
#else
	printf("\t(o_sync unavailable)  ");
#endif
	printf("\n");

	/*
	 *	Compare file sync methods with one 8k write
	 */
	printf("\nCompare file sync methods with one 8k write:\n");

#ifdef OPEN_DATASYNC_FLAG
	/* open_dsync, write */
	if ((tmpfile = open(filename, O_RDWR | O_DSYNC, 0)) == -1)
		die("Cannot open output file.");
	gettimeofday(&start_t, NULL);
	for (i = 0; i < loops; i++)
	{
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		if (lseek(tmpfile, 0, SEEK_SET) == -1)
			die("seek failed");
	}
	gettimeofday(&elapse_t, NULL);
	close(tmpfile);
	printf("\topen o_dsync, write    ");
	print_elapse(start_t, elapse_t);
#else
	printf("\t(o_dsync unavailable)  ");
#endif
	printf("\n");

#ifdef OPEN_SYNC_FLAG
	/* open_fsync, write */
	if ((tmpfile = open(filename, O_RDWR | OPEN_SYNC_FLAG, 0)) == -1)
		die("Cannot open output file.");
	gettimeofday(&start_t, NULL);
	for (i = 0; i < loops; i++)
	{
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		if (lseek(tmpfile, 0, SEEK_SET) == -1)
			die("seek failed");
	}
	gettimeofday(&elapse_t, NULL);
	close(tmpfile);
	printf("\topen o_sync, write     ");
	print_elapse(start_t, elapse_t);
#else
	printf("\t(o_sync unavailable)  ");
#endif
	printf("\n");

#ifdef HAVE_FDATASYNC
	/* write, fdatasync */
	if ((tmpfile = open(filename, O_RDWR, 0)) == -1)
		die("Cannot open output file.");
	gettimeofday(&start_t, NULL);
	for (i = 0; i < loops; i++)
	{
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		fdatasync(tmpfile);
		if (lseek(tmpfile, 0, SEEK_SET) == -1)
			die("seek failed");
	}
	gettimeofday(&elapse_t, NULL);
	close(tmpfile);
	printf("\twrite, fdatasync       ");
	print_elapse(start_t, elapse_t);
#else
	printf("\t(fdatasync unavailable)");
#endif
	printf("\n");

	/* write, fsync, close */
	if ((tmpfile = open(filename, O_RDWR, 0)) == -1)
		die("Cannot open output file.");
	gettimeofday(&start_t, NULL);
	for (i = 0; i < loops; i++)
	{
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		if (fsync(tmpfile) != 0)
			die("fsync failed");
		if (lseek(tmpfile, 0, SEEK_SET) == -1)
			die("seek failed");
	}
	gettimeofday(&elapse_t, NULL);
	close(tmpfile);
	printf("\twrite, fsync           ");
	print_elapse(start_t, elapse_t);
	printf("\n");

	/*
	 *	Compare file sync methods with two 8k write
	 */
	printf("\nCompare file sync methods with two 8k writes:\n");

#ifdef OPEN_DATASYNC_FLAG
	/* open_dsync, write */
	if ((tmpfile = open(filename, O_RDWR | O_DSYNC, 0)) == -1)
		die("Cannot open output file.");
	gettimeofday(&start_t, NULL);
	for (i = 0; i < loops; i++)
	{
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		if (lseek(tmpfile, 0, SEEK_SET) == -1)
			die("seek failed");
	}
	gettimeofday(&elapse_t, NULL);
	close(tmpfile);
	printf("\topen o_dsync, write    ");
	print_elapse(start_t, elapse_t);
#else
	printf("\t(o_dsync unavailable)  ");
#endif
	printf("\n");

#ifdef OPEN_SYNC_FLAG
	/* open_fsync, write */
	if ((tmpfile = open(filename, O_RDWR | OPEN_SYNC_FLAG, 0)) == -1)
		die("Cannot open output file.");
	gettimeofday(&start_t, NULL);
	for (i = 0; i < loops; i++)
	{
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		if (lseek(tmpfile, 0, SEEK_SET) == -1)
			die("seek failed");
	}
	gettimeofday(&elapse_t, NULL);
	close(tmpfile);
	printf("\topen o_sync, write     ");
	print_elapse(start_t, elapse_t);
	printf("\n");
#endif

#ifdef HAVE_FDATASYNC
	/* write, fdatasync */
	if ((tmpfile = open(filename, O_RDWR, 0)) == -1)
		die("Cannot open output file.");
	gettimeofday(&start_t, NULL);
	for (i = 0; i < loops; i++)
	{
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		fdatasync(tmpfile);
		if (lseek(tmpfile, 0, SEEK_SET) == -1)
			die("seek failed");
	}
	gettimeofday(&elapse_t, NULL);
	close(tmpfile);
	printf("\twrite, fdatasync       ");
	print_elapse(start_t, elapse_t);
#else
	printf("\t(fdatasync unavailable)");
#endif
	printf("\n");

	/* write, fsync, close */
	if ((tmpfile = open(filename, O_RDWR, 0)) == -1)
		die("Cannot open output file.");
	gettimeofday(&start_t, NULL);
	for (i = 0; i < loops; i++)
	{
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		if (write(tmpfile, buf, WRITE_SIZE / 2) != WRITE_SIZE / 2)
			die("write failed");
		if (fsync(tmpfile) != 0)
			die("fsync failed");
		if (lseek(tmpfile, 0, SEEK_SET) == -1)
			die("seek failed");
	}
	gettimeofday(&elapse_t, NULL);
	close(tmpfile);
	printf("\twrite, fsync           ");
	print_elapse(start_t, elapse_t);
	printf("\n");

	free(full_buf);
	unlink(filename);

	return 0;
}

void
print_elapse(struct timeval start_t, struct timeval elapse_t)
{
	if (elapse_t.tv_usec < start_t.tv_usec)
	{
		elapse_t.tv_sec--;
		elapse_t.tv_usec += 1000000;
	}

	printf("%3ld.%06ld", (long) (elapse_t.tv_sec - start_t.tv_sec),
		   (long) (elapse_t.tv_usec - start_t.tv_usec));
}

void
die(char *str)
{
	fprintf(stderr, "%s\n", str);
	exit(1);
}

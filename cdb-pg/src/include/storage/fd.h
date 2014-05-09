/*-------------------------------------------------------------------------
 *
 * fd.h
 *	  Virtual file descriptor definitions.
 *
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/storage/fd.h,v 1.56 2006/03/05 15:58:59 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */

/*
 * calls:
 *
 *	File {Close, Read, Write, Seek, Tell, Sync}
 *	{File Name Open, Allocate, Free} File
 *
 * These are NOT JUST RENAMINGS OF THE UNIX ROUTINES.
 * Use them for all file activity...
 *
 *	File fd;
 *	fd = FilePathOpenFile("foo", O_RDONLY, 0600);
 *
 *	AllocateFile();
 *	FreeFile();
 *
 * Use AllocateFile, not fopen, if you need a stdio file (FILE*); then
 * use FreeFile, not fclose, to close it.  AVOID using stdio for files
 * that you intend to hold open for any length of time, since there is
 * no way for them to share kernel file descriptors with other files.
 *
 * Likewise, use AllocateDir/FreeDir, not opendir/closedir, to allocate
 * open directories (DIR*).
 */
#ifndef FD_H
#define FD_H

#include <dirent.h>
#include "hdfs/hdfs.h"

/*
 * FileSeek uses the standard UNIX lseek(2) flags.
 */

typedef char *FileName;

typedef int File;


/* GUC parameter */
extern int	max_files_per_process;
extern bool	enable_secure_filesystem;


/*
 * prototypes for functions in fd.c
 */

/* Operations on virtual Files --- equivalent to Unix kernel file ops */

/* access local file system */
extern File LocalPathNameOpenFile(FileName fileName, int fileFlags, int fileMode);
extern void LocalFileClose(File file);
extern int LocalFileRead(File file, char *buffer, int amount);
extern int LocalFileWrite(File file, const char *buffer, int amount);
extern int64 LocalFileSeek(File file, int64 offset, int whence);
extern int LocalFileSync(File file);
extern int LocalRemovePath(FileName fileName, int recursive);
extern int LocalFileTruncate(File file, int64 offset);

/* access hdfs file system */
extern int HdfsParsePath(const char * path, char **protocol, char **host, int *port, short *replica);
extern File HdfsPathNameOpenFile(FileName fileName, int fileFlags, int fileMode);
extern void HdfsFileClose(File file, bool canReportError);
extern int HdfsFileRead(File file, char *buffer, int amount);
extern int HdfsFileWrite(File file, const char *buffer, int amount);
extern int64 HdfsFileSeek(File file, int64 offset, int whence);
extern int64 HdfsFileTell(File file);
extern int HdfsFileSync(File file);
extern int HdfsRemovePath(FileName fileName, int recursive);
extern int HdfsFileTruncate(File file, int64 offset);
extern int HdfsMakeDirectory(const char *path, mode_t mode);
extern void *HdfsGetDelegationToken(const char *uri, int *size, void **fs);
extern void HdfsRenewDelegationToken(void *fs, void *credential, int credentialSize);
extern const char * HdfsGetLastError(void);
extern void HdfsCancelDelegationToken(void *fs, void *credential, int credentialSize);
extern hdfsToken *DeserializeDelegationToken(void *binary, int size);

extern void cleanup_lru_opened_files(void);
extern void cleanup_filesystem_handler(void);

/* abstract file system */
extern File FileNameOpenFile(FileName fileName, const char *temp_dir, int fileFlags, int fileMode);
extern File PathNameOpenFile(FileName fileName, int fileFlags, int fileMode);

File
OpenTemporaryFile(const char   *fileName,
                  int           extentseqnum,
                  bool          makenameunique,
                  bool          create,
                  bool          delOnClose,
                  bool          closeAtEOXact);

File
OpenNamedFile(const char   *fileName,
                  bool          create,
                  bool          delOnClose,
                  bool          closeAtEOXact);

extern void FileClose(File file);
extern void FileUnlink(File file);
extern int	FileRead(File file, char *buffer, int amount);
extern int	FileReadIntr(File file, char *buffer, int amount, bool fRetryInt);
extern int	FileWrite(File file, const char *buffer, int amount);
extern int	FileSync(File file);
extern int64 FileSeek(File file, int64 offset, int whence);
extern int64 FileNonVirtualTell(File file);
extern int	FileTruncate(File file, int64 offset);
extern int64 FileDiskSize(File file);

/* Operations that allow use of regular stdio --- USE WITH CAUTION */
extern FILE *AllocateFile(const char *name, const char *mode);
extern int	FreeFile(FILE *file);

/* Operations to allow use of the <dirent.h> library routines */
extern DIR *AllocateDir(const char *dirname);
extern struct dirent *ReadDir(DIR *dir, const char *dirname);
extern int	FreeDir(DIR *dir);

/* If you've really really gotta have a plain kernel FD, use this */
extern int	BasicOpenFile(FileName fileName, int fileFlags, int fileMode);

/* Miscellaneous support routines */
extern void InitFileAccess(void);
extern void set_max_safe_fds(void);
extern void closeAllVfds(void);
extern void AtEOXact_Files(void);
extern void AtEOSubXact_Files(bool isCommit, SubTransactionId mySubid,
				  SubTransactionId parentSubid);
extern void RemovePgTempFiles(void);
extern int	pg_fsync(int fd);
extern int	pg_fsync_no_writethrough(int fd);
extern int	pg_fsync_writethrough(int fd);
extern int	pg_fdatasync(int fd);
extern int  gp_retry_close(int fd);
extern int  RemovePath(FileName fileName, int recursive);
extern int  MakeDirectory(const char * path, mode_t mode);
extern char *make_database_relative(const char *filename);

/* Filename components for OpenTemporaryFile */
#define PG_TEMP_FILES_DIR "pgsql_tmp"
#define PG_TEMP_FILE_PREFIX "pgsql_tmp"

extern size_t GetTempFilePrefix(char * buf, size_t buflen, const char * fileName);

extern bool TestFileValid(File file);

extern bool HdfsPathExist(char *path);

extern int64 HdfsPathSize(DIR *dirdesc);

extern FileName FileGetName(File file);

extern int IsLocalPath(const char *filename);

/* secure enabled hdfs */

#endif   /* FD_H */

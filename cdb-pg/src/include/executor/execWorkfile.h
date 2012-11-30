/*-------------------------------------------------------------------------
 *
 * execworkfile.h
 *    prototypes for execworkfiles.c
 *
 * Copyright (c) 2010, Greenplum Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXECWORKFILE_H
#define EXECWORKFILE_H

typedef enum ExecWorkFileType
{
	BUFFILE = 0,
	BFZ
} ExecWorkFileType;

/*
 * The size of a single write that OS disk drivers guaranteed to write to
 * disk.
 */ 
#define WORKFILE_SAFEWRITE_SIZE 512

/* 
 * ExecWorkFile structure.
 */
typedef struct ExecWorkFile
{
	ExecWorkFileType fileType;
	int compressType;
	
	void *file;
} ExecWorkFile;

/*
 * ExecWorkFile_Create
 *    create a new work file with the specified name, the file type,
 * and the compression type.
 *
 * If this fails, NULL is returned.
 */
ExecWorkFile *
ExecWorkFile_Create(const char *fileName,
					ExecWorkFileType fileType,
					int compressType);

/*
 * ExecWorkFile_Write
 *    write the given data from the end of the last write position.
 *
 * This function returns true if the write succeeds. Otherwise, return false.
 */
bool
ExecWorkFile_Write(ExecWorkFile *workfile,
				   void *data,
				   uint64 size);

/*
 * ExecWorkFile_Read
 *    read the data with specified size to the given buffer.
 *
 * The given buffer should contain at least the space specified by 
 * 'size'.
 *
 * If the read succeeds, this function returns the number of bytes
 * that are read. Otherwise, returns 0.
 */
uint64
ExecWorkFile_Read(ExecWorkFile *workfile,
				  void *data,
				  uint64 size);

/*
 * ExecWorkFile_Rewind
 *    rewind the pointer position to the beginning of the file.
 *
 * This function returns true if this succeeds. Otherwise, return false.
 */
bool
ExecWorkFile_Rewind(ExecWorkFile *workfile);

/*
 * ExecWorkFile_Tell64
 *    return the value of the current file position indicator.
 */
uint64
ExecWorkFile_Tell64(ExecWorkFile *workfile);

/*
 * ExecWorkFile_Close
 *    close the work file, and release the space.
 */
void
ExecWorkFile_Close(ExecWorkFile *workfile);

#endif /* EXECWORKFILE_H */
